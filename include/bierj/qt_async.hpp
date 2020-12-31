#pragma once

#ifndef BIERJ_QT_ASYNC_H
#define BIERJ_QT_ASYNC_H

#include <any>
#include <bierj/channels.hpp>
#include <bierj/threadpool.hpp>
#include <exception>
#include <functional>
#include <map>
#include <mutex>
#include <QApplication>
#include <QDebug>
#include <QEvent>
#include <QObject>
#include <QTimer>
#include <set>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

namespace bierj
{

namespace qt
{

namespace detail
{

template<class K, class V> using dict = std::unordered_map<K, V, std::hash<K>, std::equal_to<K>, std::allocator<std::pair<const K, V>>>;

inline QEvent::Type delayed_event_type()
{
    static QEvent::Type type = static_cast<QEvent::Type>(QEvent::registerEventType());
    return type;
}

class delayed : public QEvent
{
public:
    delayed(std::function<void()> &&delayed) : QEvent(delayed_event_type()), _delayed(std::move(delayed)) {}

    void run() { _delayed(); }

private:
    std::function<void()> _delayed;
};

enum class interrupt_reason
{
    register_async_function,
    register_async_connection,
    register_continuation,
    deregister_continuation,
    timeout
};

} // end namespace bierj::qt::detail

enum class launch_policy
{
    thread_pool,
    new_thread
};

struct already_observing : public std::exception {};

template<class WrappedObject> class async_executor;

template<class WrappedObject, bool CanTimeout, class Sender, class PointerToMemberFn, class... ContParams>
class async_connection;

// Subscribed prevents .with_timeout being called on the retval of .then since a call to .then starts the async_function and would permit a race condition
template<class WrappedObject, bool CanTimeout, class... ContParams>
class async_function
{
    using executor = async_executor<WrappedObject>;

public:
    async_function(executor *ex, any_channel ch)
        : m_ex(ex), m_ch(ch)
    {}

    virtual ~async_function()
    {
        m_ex->deregister_continuation(m_ch);
    }

    auto with_timeout(int timeout)
    {
        static_assert(CanTimeout);

        auto timer = make_shared(new QTimer());
        timer->setSingleShot(true);
        timer->setInterval(timeout);

        m_ex->subscribe(timer, &QTimer::timeout).until([]() { })

        timer->start();

        return then([=](channel<ContParams...> ch) mutable
        {
            // restart timer each time a value comes in
            timer->start();
        });
    }

    // registers a continuation on this async function which generates another async function
    template
    <
        class... Cont2Params,
        class Fn,
        class = std::enable_if_t
        <
            std::is_invocable_v
            <
                std::invoke_result_t<Fn, ContParams...>,
                channel<Cont2Params...>
            >
        >
    >
    auto then(Fn &&f, launch_policy policy = launch_policy::thread_pool)
    {
        channel<Cont2Params...> ch;
        any_channel wrapped_ch(ch);

        // capture a copy of the executor pointer by value
        // because *this may not be valid when the continuation runs
        executor *ex = m_ex;

        std::function<void(std::any)> cont = [=](std::any result)
        {
            // run continuation
            auto args = std::any_cast<std::tuple<ContParams...>>(result);
            auto g = std::apply(f, args);
            ex->continuation_complete();

            // register result as an async function
            ex->register_async_function(wrapped_ch, [=]() mutable { g(ch); ch.close(); }, policy);
        };

        // as a side effect, register continuation starts the associated async function based on policy
        m_ex->register_continuation(m_ch, cont);

        return async_function<WrappedObject, false, Cont2Params...>(m_ex, wrapped_ch);
    }

    // registers a continuation on this async result which generates an async connection
    template
    <
        class Fn,
        class Sender,
        class PointerToMemberFn,
        class... Cont2Params,
        class = std::enable_if_t
        <
            std::is_invocable_v
            <
                std::invoke_result_t<Fn, ContParams...>,
                std::tuple<std::shared_ptr<Sender>, PointerToMemberFn>
            >
        >
    >
    auto then(Fn &&f)
    {
        channel<Cont2Params...> ch;
        any_channel wrapped_ch(ch);

        // capture a copy of the executor pointer by value
        // because *this may not be valid when the continuation runs
        executor *ex = m_ex;

        std::function<QMetaObject::Connection()> cont = [=](std::any result)
        {
            // run continuation
            auto args = std::any_cast<std::tuple<ContParams...>>(result);
            auto [sender, signal] = std::apply(f, args);
            ex->continuation_complete();

            // register result as a connection
            ex->register_async_connection(wrapped_ch, [=]() mutable
            {
                return QObject::connect(sender, signal, [=](ContParams... args)
                {
                    ch.send(args...);
                });
            });
        };

        m_ex->register_continuation(m_ch, cont);

        return async_connection<WrappedObject, false, Sender, PointerToMemberFn, Cont2Params...>(m_ex, wrapped_ch);
    }

    // registers a continuation on this async result which yields no further async results
    template
    <
        class Fn,
        class = std::enable_if_t
        <
            std::is_same_v
            <
                std::invoke_result_t<Fn, ContParams...>,
                void
            >
        >
    >
    auto then(Fn &&f)
    {
        // capture a copy of the executor pointer by value
        // because *this may not be valid when the continuation runs
        executor *ex = m_ex;

        std::function<void(std::any)> cont = [=](std::any result)
        {
            // run continuation
            auto args = std::any_cast<std::tuple<ContParams...>>(result);
            std::apply(f, args);
            ex->continuation_complete();
        };

        // register continuation which starts the associated async function
        m_ex->register_continuation(m_ch, cont);

        // return async function to enable chaining
        return *this;
    }

protected:
    executor *m_ex;
    any_channel m_ch;
};

template<class WrappedObject, bool CanTimeout, class Sender, class PointerToMemberFn, class... ContParams>
class async_connection : public async_function<WrappedObject, CanTimeout, ContParams...>
{
    using executor = async_executor<WrappedObject>;
    using base = async_function<WrappedObject, CanTimeout, ContParams...>;

public:
    async_connection(executor *ex, std::shared_ptr<Sender> sender, PointerToMemberFn signal, any_channel ch)
        : base::m_ex(ex), base::m_ch(ch), m_sender(sender), m_signal(signal)
    {}

    ~async_connection()
    {
        base::m_ex->deregister_continuation(base::m_ch);
    }

    // disconnects the async connection once this signal is fired and returns an async connection
    template
    <
        class Sender2,
        class PointerToMemberFn2,
        class... Cont2Params
    >
    auto until(std::shared_ptr<Sender2> sender, PointerToMemberFn2 signal)
    {
        channel<Cont2Params...> ch;
        any_channel wrapped_ch(ch);

        // capture a copy of the executor pointer by value
        // because *this may not be valid when the continuation runs
        executor *ex = base::m_ex;

        // register new signal as an async connection
        ex->register_async_connection(wrapped_ch, [=]() mutable
        {
            QMetaObject::Connection con;
            con = QObject::connect(sender, signal, [=](Cont2Params... args)
            {
                this->m_ch.close();
                QObject::disconnect(con);
            });

            return QObject::connect(sender, signal, [=](Cont2Params... args)
            {
                ch.send(args...);
            });
        });

        base::m_ex->register_continuation(base::m_ch, cont);

        return async_connection<WrappedObject, true, Cont2Params...>(base::m_ex, wrapped_ch);
    }

    // disconnects the async connection once the given predicate returns true
    template<class Predicate>
    void until(Predicate &&pred)
    {
        // capture a copy of the executor pointer by value
        // because *this may not be valid when the continuation runs
        executor *ex = base::m_ex;

        std::function<QMetaObject::Connection()> cont = [=](std::any result)
        {
            auto args = std::any_cast<std::tuple<ContParams...>>(result);
            if (std::apply(pred, args))
            {
                // close the channel to signal the executor to deregister the connection
                this->m_ch.close();
            }

            ex->continuation_complete();
        };

        base::m_ex->register_continuation(base::m_ch, cont);
    }

protected:
    std::shared_ptr<Sender> m_sender;
    PointerToMemberFn m_signal;
};

template<class WrappedObject>
class async_executor : public WrappedObject
{
public:
    async_executor(QWidget *parent) : WrappedObject(parent), interrupt_chan(buffered_channel<detail::interrupt_reason>{})
    {
        result_chans.insert(interrupt_chan);

        // listens for results from async functions, registration of new async functions, registration of continuations to async functions,
        // and unregistration of async functions which have completed (which also unregister the associated continuation).
        // continuations run in the GUI thread.
        std::thread([this]
        {
            try
            {
                qDebug() << "bier::qt::async_executor: starting continuation listener";
                for (;;)
                {
                    auto ch = select(result_chans.begin(), result_chans.end());
                    if (*ch == interrupt_chan)
                    {
                        auto [reason] = std::any_cast<std::tuple<detail::interrupt_reason>>(ch->receive());
                        if (reason == detail::interrupt_reason::register_async_function)
                        {
                            auto [f, policy, ch] = register_async_chan.receive();
                            async_standby[ch] = {f, policy};
                            result_chans.insert(ch);
                        }
                        else if (reason == detail::interrupt_reason::register_async_connection)
                        {
                            auto [f, ch] = register_connection_chan.receive();
                            connections[ch] = f();
                            result_chans.insert(ch);
                        }
                        else if (reason == detail::interrupt_reason::register_continuation)
                        {
                            auto [f, ch] = register_cont_chan.receive();
                            cont_standby[ch] = f;

                            if (async_standby.count(ch) > 0)
                            {
                                auto &[async_function, policy] = async_standby[ch];
                                        if (policy == launch_policy::thread_pool)
                                {
                                    pool.send(std::move(async_function));
                                }
                                else
                                {
                                    std::thread(async_function).detach();
                                }
                                async_standby.erase(ch);
                            }
                            // no need to do anything for async connections
                        }
                        else if (reason == detail::interrupt_reason::deregister_continuation)
                        {
                            auto [ch] = deregister_chan.receive();
                            cont_standby.erase(ch);
                            result_chans.erase(ch);
                            if (connections.count(ch) > 0)
                            {
                                QObject::disconnect(connections[ch]);
                                connections.erase(ch);
                            }
                        }
                        else if (reason == detail::interrupt_reason::timeout)
                        {

                        }
                    }
                    else
                    {
                        auto result = ch->receive();
                        // we can safely capture by reference because we block until the continuation has completed
                        auto f = [this, &ch, &result] { cont_standby[*ch](std::move(result)); };
                        auto cont = new detail::delayed(f);
                        // we can't use QApplication::sendEvent because that can only be called from the GUI thread
                        // so instead we asynchronously post it from this thread and wait until the continuation runs
                        // by waiting on the continuation completion channel
                        QApplication::postEvent(this, cont);
                        cont_complete_chan.receive();

                        if (ch->is_closed())
                        {
                            interrupt_chan.send(detail::interrupt_reason::deregister_continuation);
                            deregister_chan.send(*ch);
                        }
                    }
                }
            }
            catch (all_channels_closed)
            {
                qDebug() << "bier::qt::async_executor: continuation listener exiting";
            }
        }).detach();
    }

    ~async_executor()
    {
        register_async_chan.close();
        register_cont_chan.close();
        deregister_chan.close();
        pool.join();
    }

protected:
    bool event(QEvent *e) override
    {
        if (e->type() == detail::delayed_event_type())
        {
            reinterpret_cast<detail::delayed *>(e)->run();
            return true;
        }
        return WrappedObject::event(e);
    }

    template<class... ContParams, class Fn>
    auto async(Fn &&f, launch_policy policy = launch_policy::thread_pool)
    {
        channel<ContParams...> ch;
        any_channel wrapped_ch(ch);
        register_async_function(wrapped_ch, [=]() mutable { f(ch); ch.close(); }, policy);
        return async_function<WrappedObject, true, ContParams...>(this, wrapped_ch);
    }

    template<class Sender, class PointerToMemberFn, class... ContParams>
    auto subscribe(std::shared_ptr<Sender> sender, PointerToMemberFn signal)
    {
        channel<ContParams...> ch;
        any_channel wrapped_ch(ch);
        register_async_connection(wrapped_ch, [=]() mutable
        {
            return QObject::connect(sender, signal, [=](ContParams... args)
            {
                ch.send(args...);
            });
        });
        return async_connection<WrappedObject, true, ContParams...>(this, wrapped_ch);
    }

    //template<class... AsyncResults, class... ContParams, class Fn>
    //auto when_all(AsyncResults &&... futures, Fn &&f)
    //{
    //}

    //// Sequence must be iterable
    //template<class Result, template<class> class Sequence, class... ContParams, class Fn>
    //auto when_all(Sequence<Result> &&futures, Fn &&f)
    //{
    //}

    //template<class... AsyncResults, class... ContParams, class Fn>
    //auto when_any(AsyncResults &&... futures, Fn &&f)
    //{
    //}

    //// Sequence must be iterable
    //template<class Result, template<class> class Sequence, class... ContParams, class Fn>
    //auto when_any(Sequence<Result> &&futures, Fn &&f)
    //{
    //}

    template<class Fn>
    void run_on_gui(Fn &&f)
    {
        auto thunk = new detail::delayed([f]() { f(); });
        QApplication::postEvent(this, thunk);
    }

private:
    void register_async_function(const any_channel &ch, std::function<void()> fn, launch_policy policy)
    {
        interrupt_chan.send(detail::interrupt_reason::register_async_function);
        register_async_chan.send(move(fn), policy, ch);
    }

    void register_async_connection(const any_channel &ch, std::function<QMetaObject::Connection()> fn)
    {
        interrupt_chan.send(detail::interrupt_reason::register_async_connection);
        register_connection_chan.send(move(fn), ch);
    }

    void register_continuation(const any_channel &ch, std::function<void(std::any)> cont)
    {
        interrupt_chan.send(detail::interrupt_reason::register_continuation);
        register_cont_chan.send(cont, ch);
    }

    void deregister_continuation(const any_channel &ch)
    {
        interrupt_chan.send(detail::interrupt_reason::deregister_continuation);
        deregister_chan.send(ch);
    }

    void continuation_complete()
    {
        cont_complete_chan.send();
    }

    buffered_threadpool pool;

    detail::dict<any_channel, std::tuple<std::function<void()>, launch_policy>> async_standby;
    detail::dict<any_channel, std::function<QMetaObject::Connection()>> connection_standby;
    detail::dict<any_channel, std::function<void(std::any)>> cont_standby;

    detail::dict<any_channel, QMetaObject::Connection> connections;

    std::unordered_set<any_channel> result_chans;
    any_channel interrupt_chan;

    buffered_channel<std::function<void()>, launch_policy, any_channel> register_async_chan;
    buffered_channel<std::function<QMetaObject::Connection()>, any_channel> register_connection_chan;
    buffered_channel<std::function<void(std::any)>, any_channel> register_cont_chan;
    buffered_channel<any_channel> deregister_chan;

    channel<> cont_complete_chan;

    template<class WrappedObject, bool CanTimeout, class... ContParams> friend class async_function;
    template<class WrappedObject, bool CanTimeout, class Sender, class PointerToMemberFn, class... ContParams> friend class async_connection;
};

} // end namespace bierj::qt

} // end namespace bierj

#endif
