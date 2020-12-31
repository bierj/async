#pragma once

#ifndef BIERJ_CHANNELS_HPP
#define BIERJ_CHANNELS_HPP

#include <any>
#include <condition_variable>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <tuple>
#include <utility>

namespace bierj
{

class channel_closed : std::exception {};
class all_channels_closed : std::exception {};

namespace detail
{

struct select_observer
{
    select_observer() : ready(false), expired(false) {}
    select_observer(const select_observer &) = delete;
    select_observer(select_observer &&) = delete;
    std::mutex m;
    std::condition_variable cond;
    size_t readied_channel;
    bool ready;
    bool expired;
};

struct select_registration
{
    select_registration(const std::shared_ptr<select_observer> &observer, size_t id) : observer(observer), id(id) {}
    std::shared_ptr<select_observer> observer;
    size_t id;
};

// invoke and apply definitions are temporary until MSVC supports std::apply
// code taken from http://en.cppreference.com/w/cpp/utility/functional/invoke
// and http://en.cppreference.com/w/cpp/utility/apply

//template<class F, class... Args>
//auto invoke(F &&f, Args &&...args)
//{
//	return forward<F>(f)(forward<Args>(args)...);
//}

//template<class F, class Tuple, size_t... I>
//auto apply_impl(F &&f, Tuple &&t, index_sequence<I...>)
//{
//	return detail::invoke(forward<F>(f), get<I>(forward<Tuple>(t))...);
//}

//template<class F, class Tuple>
//auto apply(F &&f, Tuple &&t)
//{
//	return apply_impl(forward<F>(f), forward<Tuple>(t), make_index_sequence<tuple_size<decay_t<Tuple>>::value>{});
//}

// taken from http://stackoverflow.com/questions/8194227/how-to-get-the-i-th-element-from-an-stdtuple-when-i-isnt-know-at-compile-time
template<size_t I = 0, class Fn, class... T>
std::enable_if_t<I == sizeof...(T), void> tuple_for(size_t, std::tuple<T...> &, Fn) {}

template<size_t I = 0, class Fn, class... T>
std::enable_if_t<I < sizeof...(T), void> tuple_for(size_t idx, std::tuple<T...> &t, Fn f)
{
    if (I == idx) f(std::get<I>(t));
    else tuple_for<I + 1, Fn, T...>(idx, t, f);
}

template<size_t I = 0, class Fn, class... T>
std::enable_if_t<I == sizeof...(T) / 2, void> tuple_for2(size_t, std::tuple<T...> &, Fn) {}

template<size_t I = 0, class Fn, class... T>
std::enable_if_t<I < sizeof...(T) / 2, void> tuple_for2(size_t idx, std::tuple<T...> &t, Fn f)
{
    static_assert(sizeof...(T) % 2 == 0, "tuple length not divisible by 2");
    if (I == idx) f(std::get<I>(t), std::get<I + sizeof...(T) / 2>(t));
    else tuple_for2<I + 1, Fn, T...>(idx, t, f);
}

template<size_t I = 0, class Fn, class... T>
std::enable_if_t<I == sizeof...(T), bool> tuple_foreach(std::tuple<T...> &, Fn)
{
    return false;
}

template<size_t I = 0, class Fn, class... T>
std::enable_if_t<I < sizeof...(T), bool> tuple_foreach(std::tuple<T...> &t, Fn f)
{
    bool stop = false;
    f(I, std::get<I>(t), stop);
    if (stop) return true;
    else return tuple_foreach<I + 1, Fn, T...>(t, f);
}

template<size_t I = 0, class Fn, class... T>
std::enable_if_t<I == sizeof...(T) / 2, bool> tuple_foreach2(std::tuple<T...> &, Fn)
{
    return false;
}

template<size_t I = 0, class Fn, class... T>
std::enable_if_t<I < sizeof...(T) / 2, bool> tuple_foreach2(std::tuple<T...> &t, Fn f)
{
    static_assert(sizeof...(T) % 2 == 0, "tuple length not divisible by 2");
    bool stop = false;
    f(I, std::get<I>(t), std::get<I + sizeof...(T) / 2>(t), stop);
    if (stop) return true;
    else return tuple_foreach2<I + 1, Fn, T...>(t, f);
}

} // end namespace bierj::detail

template<class... T>
class channel
{
    typedef std::unique_lock<std::mutex> lock_type;

    struct state
    {
        mutable std::mutex m;
        std::condition_variable sent, received;
        // ready means a message has been sent
        // valid means a message is waiting to be received
        // done means the channel is closing and no more message can be sent
        // done and not valid means the channel is closed
        bool ready, valid, done;
        std::tuple<T...> msg;
        std::deque<detail::select_registration> registration;
    };

public:
    channel()
    {
        s = std::make_shared<state>();
        s->ready = false;
        s->valid = false;
        s->done = false;
    }

    channel(channel<T...> &&other) : s(move(other.s)) {}

    channel(const channel<T...> &other) : s(other.s) {}

    void send(T... msg)
    {
        {
            lock_type lock{s->m};

            if_closing_throw(lock);

            while (!s->done && s->ready) s->received.wait(lock);

            if_closing_throw(lock);

            s->msg = std::make_tuple(std::move(msg)...);
            s->valid = true;
            s->ready = true;

            notify_observers_nolock();
        }
        s->sent.notify_one();
    }

    std::tuple<T...> receive()
    {
        std::tuple<T...> msg;
        {
            lock_type lock{s->m};

            if_closed_throw(lock);

            while (!s->done && !s->ready) s->sent.wait(lock);

            if_closed_throw(lock);

            msg = std::move(s->msg);
            s->valid = false;
            s->ready = false;

            if (s->done) s->sent.notify_one();
        }
        s->received.notify_one();
        return msg;
    }

    std::optional<std::tuple<T...>> try_receive()
    {
        std::tuple<T...> msg;
        {
            lock_type lock{s->m};

            if_closed_throw(lock);

            if (!s->ready) return {};

            msg = std::move(s->msg);
            s->valid = false;
            s->ready = false;

            if (s->done) s->sent.notify_one();
        }
        s->received.notify_one();
        return std::make_optional(msg);
    }

    void reopen()
    {
        {
            lock_type lock{s->m};
            if (!is_closed_nolock()) return;
            s->ready = false;
            s->valid = false;
            s->done = false;
            s->msg = {};
            s->registration.clear();
        }
    }

    void close()
    {
        {
            lock_type lock{s->m};
            if (is_closing_nolock()) return;
            s->ready = true;
            s->done = true;

            notify_observers_nolock();
        }
        s->sent.notify_one();
        s->received.notify_one();
    }

    bool is_closing() { lock_type lock(s->m); return s->done; }
    bool is_closed() { lock_type lock(s->m); return s->done && !s->valid; }

private:
    bool ready_nolock() const noexcept { return s->ready; }

    bool is_closing_nolock() const noexcept { return s->done; }
    bool is_closed_nolock() const noexcept { return s->done && !s->valid; }

    void if_closing_throw(lock_type &lock)
    {
        if (is_closing_nolock())
        {
            notify_observers_nolock();
            lock.unlock();
            s->sent.notify_one();
            s->received.notify_one();
            throw channel_closed{};
        }
    }

    void if_closed_throw(lock_type &lock)
    {
        if (is_closed_nolock())
        {
            notify_observers_nolock();
            lock.unlock();
            s->sent.notify_one();
            s->received.notify_one();
            throw channel_closed{};
        }
    }

    void register_observer_nolock(const detail::select_registration &observer)
    {
        s->registration.push_back(observer);
    }

    void notify_observers_nolock()
    {
        while (s->registration.size() > 0)
        {
            auto reg = s->registration.front();
            bool expired = false;
            {
                std::lock_guard<std::mutex> lock{reg.observer->m};
                expired = reg.observer->expired;
                if (!expired && !reg.observer->ready)
                {
                    reg.observer->readied_channel = reg.id;
                    reg.observer->ready = true;
                }
            }
            if (!expired) reg.observer->cond.notify_one();
            s->registration.pop_front();
        }
    }

    std::tuple<T...> take_msg_nolock()
    {
        std::tuple<T...> msg{std::move(s->msg)};
        s->valid = false;
        s->ready = false;
        return msg;
    }

    std::shared_ptr<state> s;

    template<class... Channels, class... Visitors> friend void select(std::tuple<Channels...> &&, Visitors...);

    friend class any_channel;
};

template<class... T>
class buffered_channel
{
    typedef std::unique_lock<std::mutex> lock_type;

    struct state
    {
        mutable std::mutex m;
        std::condition_variable not_full, not_empty;
        bool done;
        size_t capacity;
        std::deque<std::tuple<T...>> queue;
        std::deque<detail::select_registration> registration;
    };

public:
    buffered_channel(size_t capacity = 0)
    {
        s = std::make_shared<state>();
        s->done = false;
        s->capacity = capacity;
    }

    buffered_channel(buffered_channel<T...> &&other) : s(std::move(other.s)) {}

    buffered_channel(const buffered_channel<T...> &other) : s(other.s) {}

    size_t send(T... msg)
    {
        size_t size;
        {
            lock_type lock{s->m};

            if_closing_throw(lock);

            while (!s->done && s->capacity > 0 && s->queue.size() > s->capacity)
                s->not_full.wait(lock);

            if_closing_throw(lock);

            s->queue.emplace_back(std::move(msg)...);
            size = s->queue.size();

            notify_observers_nolock();
        }
        s->not_empty.notify_one();
        return size;
    }

    std::tuple<T...> receive()
    {
        std::tuple<T...> msg;
        {
            lock_type lock{s->m};

            if_closed_throw(lock);

            while (!s->done && s->queue.size() == 0)
                s->not_empty.wait(lock);

            if_closed_throw(lock);

            msg = std::move(s->queue.front());
            s->queue.pop_front();

            if (s->done) s->not_empty.notify_one();
        }
        s->not_full.notify_one();
        return msg;
    }

    std::optional<std::tuple<T...>> try_receive()
    {
        std::tuple<T...> msg;
        {
            lock_type lock{s->m};

            if_closed_throw(lock);

            if (s->queue.size() == 0) return {};

            msg = std::move(s->queue.front());
            s->queue.pop_front();

            if (s->done) s->not_empty.notify_one();
        }
        s->not_full.notify_one();
        return std::make_optional(msg);
    }

    void reopen()
    {
        {
            lock_type lock{s->m};
            if (!is_closed_nolock()) return;
            s->done = false;
            s->queue.clear();
            s->registration.clear();
        }
    }

    void close()
    {
        {
            lock_type lock{s->m};
            if (is_closing_nolock()) return;
            s->done = true;

            notify_observers_nolock();
        }
        s->not_empty.notify_one();
        s->not_full.notify_one();
    }

    bool is_closing() { lock_type lock(s->m); return s->done; }
    bool is_closed() { lock_type lock(s->m); return s->done && s->queue.size() == 0; }

private:
    bool ready_nolock() const noexcept { return s->queue.size() > 0; }

    bool is_closing_nolock() const noexcept { return s->done; }
    bool is_closed_nolock() const noexcept { return s->done && s->queue.size() == 0; }

    void if_closing_throw(lock_type &lock)
    {
        if (is_closing_nolock())
        {
            notify_observers_nolock();
            lock.unlock();
            s->not_empty.notify_one();
            s->not_full.notify_one();
            throw channel_closed{};
        }
    }

    void if_closed_throw(lock_type &lock)
    {
        if (is_closed_nolock())
        {
            notify_observers_nolock();
            lock.unlock();
            s->not_empty.notify_one();
            s->not_full.notify_one();
            throw channel_closed{};
        }
    }

    void register_observer_nolock(const detail::select_registration &observer)
    {
        s->registration.push_back(observer);
    }

    void notify_observers_nolock()
    {
        while (s->registration.size() > 0)
        {
            auto reg = s->registration.front();
            bool expired = false;
            {
                std::lock_guard<std::mutex> lock{reg.observer->m};
                expired = reg.observer->expired;
                if (!expired && !reg.observer->ready)
                {
                    reg.observer->readied_channel = reg.id;
                    reg.observer->ready = true;
                }
            }
            if (!expired) reg.observer->cond.notify_one();
            s->registration.pop_front();
        }
    }

    std::tuple<T...> take_msg_nolock()
    {
        std::tuple<T...> msg{std::move(s->queue.front())};
        s->queue.pop_front();
        return msg;
    }

    std::shared_ptr<state> s;

    template<class... Channels, class... Visitors> friend void select(std::tuple<Channels...> &&, Visitors...);

    friend class any_channel;
};

template<class... Channels, class... Visitors>
void select(std::tuple<Channels &...> &&channels, Visitors... visitors)
{
    static_assert(sizeof...(Channels) <= sizeof...(Visitors), "too many channels");
    static_assert(sizeof...(Channels) >= sizeof...(Visitors), "too many visitors");

    //lock_cout(cout << "Thread " << this_thread::get_id() << ": entering select...\n");

    std::tuple<Visitors...> _visitors{visitors...};

    auto channels_and_visitors = std::tuple_cat(channels, _visitors);

    auto obs = std::make_shared<detail::select_observer>();

    size_t open_channels = sizeof...(Channels);

    std::unique_lock<std::mutex> obs_lock{obs->m};

    // first check if any channel is ready while removing any closed channels
    bool stop = detail::tuple_foreach2(channels_and_visitors, [&](size_t i, auto &ch, auto &fn, bool &stop)
    {
            std::unique_lock<std::mutex> ch_lock{ch.s->m};

            //lock_cout(cout << "Thread " << this_thread::get_id() << ": checking channel " << i << '\n');

            if (ch.ready_nolock())
    {
            //lock_cout(cout << "Thread " << this_thread::get_id() << ": channel " << i << " ready\n");
            obs->expired = true;
            obs_lock.unlock();
            auto msg = ch.take_msg_nolock();
            ch_lock.unlock();
            stop = true;
            std::apply(fn, msg);
}
            else if (ch.is_closed_nolock())
    {
            //lock_cout(cout << "Thread " << this_thread::get_id() << ": channel " << i << " closed\n");
            open_channels--;
}
            else
    {
            //lock_cout(cout << "Thread " << this_thread::get_id() << ": registering observer on channel " << i << '\n');
            ch.register_observer_nolock(detail::select_registration{obs, i});
}
});

    if (stop)
    {
        //lock_cout(cout << "Thread " << this_thread::get_id() << ": exiting select\n");
        return;
    }

    if (open_channels == 0)
    {
        obs->expired = true;
        throw all_channels_closed{};
    }

    // check if readied channel is closed when notified
    // if closed then wait for another channel unless all the channels are closed
    for (;;)
    {
        if (open_channels == 0)
        {
            obs->expired = true;
            throw all_channels_closed{};
        }

        //lock_cout(cout << "Thread " << this_thread::get_id() << ": waiting...\n");
        while (!obs->ready)
            obs->cond.wait(obs_lock);
        //lock_cout(cout << "Thread " << this_thread::get_id() << ": awake!\n");

        size_t id = obs->readied_channel;
        //lock_cout(cout << "Thread " << this_thread::get_id() << ": readied channel = " << id << "\n");

        bool stop = false;
        detail::tuple_for2(id, channels_and_visitors, [&](auto &ch, auto &fn)
        {
            //lock_cout(cout << "Thread " << this_thread::get_id() << ": locking channel " << id << "\n");
            std::unique_lock<std::mutex> ch_lock{ch.s->m};

            if (ch.is_closed_nolock())
            {
                //lock_cout(cout << "Thread " << this_thread::get_id() << ": channel " << id << " closed\n");
                open_channels--;
            }
            else
            {
                //lock_cout(cout << "Thread " << this_thread::get_id() << ": channel " << id << " ready\n");
                obs->expired = true;
                obs_lock.unlock();
                auto msg = ch.take_msg_nolock();
                ch_lock.unlock();
                std::apply(fn, msg);
                stop = true;
            }
        });
        if (stop) break;
    }
    //lock_cout(cout << "Thread " << this_thread::get_id() << ": exiting select\n");
}

template<class... Channels, class... Visitors>
bool try_select(std::tuple<Channels...> &&channels, Visitors... visitors)
{
    static_assert(sizeof...(Channels) <= sizeof...(Visitors), "too many channels");
    static_assert(sizeof...(Channels) >= sizeof...(Visitors), "too many visitors");

    //lock_cout(cout << "Thread " << this_thread::get_id() << ": entering select...\n");

    std::tuple<Visitors...> _visitors{visitors...};

    auto channels_and_visitors = std::tuple_cat(channels, _visitors);

    size_t open_channels = sizeof...(Channels);

    // check if any channel is ready while removing any closed channels
    bool stop = detail::tuple_foreach2(channels_and_visitors, [&](size_t i, auto &ch, auto &fn, bool &stop)
    {
            std::unique_lock<std::mutex> ch_lock{ch.s->m};

            //lock_cout(cout << "Thread " << this_thread::get_id() << ": checking channel " << i << '\n');

            if (ch.ready_nolock())
    {
            //lock_cout(cout << "Thread " << this_thread::get_id() << ": channel " << i << " ready\n");
            auto msg = ch.take_msg_nolock();
            ch_lock.unlock();
            stop = true;
            std::apply(fn, msg);
}
            else if (ch.is_closed_nolock())
    {
            //lock_cout(cout << "Thread " << this_thread::get_id() << ": channel " << i << " closed\n");
            open_channels--;
}
});

    if (open_channels == 0)
    {
        throw all_channels_closed{};
    }

    // if stop is true then a ready channel was found otherwise no channel was ready but some were still open
    return stop;
}

class empty_any_channel : public std::exception {};

// adapter to support type erasure for channels
class any_channel
{
public:
    any_channel() : m(nullptr), ptr(nullptr) {}

    template<template<class...> class Channel, class... T>
    any_channel(Channel<T...> ch) : _ch(ch), m(&ch.s->m), ptr(ch.s.get())
    {
        _send = [=](std::any &&msg) { std::apply([ch](T... params) mutable { ch.send(params...); }, std::any_cast<std::tuple<T...>>(msg)); };
        _receive = [=]() mutable { return ch.receive(); };
        _ready = [=]() { return ch.ready_nolock(); };
        _is_closing = [=]() { return ch.is_closing_nolock(); };
        _is_closed = [=]() { return ch.is_closed_nolock(); };
        _take = [=]() mutable { return std::any(ch.take_msg_nolock()); };
        _register = [=](detail::select_registration &&reg) mutable { ch.register_observer_nolock(std::move(reg)); };
    }

    template<class... T>
    void send(T... msg) const
    {
        if_has_no_value_throw();
        _send(std::tuple<T...>{msg...});
    }

    std::any receive() const
    {
        if_has_no_value_throw();
        return _receive();
    }

    bool is_closing() const
    {
        if_has_no_value_throw();
        std::lock_guard<std::mutex> lock(*m);
        return _is_closing();
    }

    bool is_closed() const
    {
        if_has_no_value_throw();
        std::lock_guard<std::mutex> lock(*m);
        return _is_closed();
    }

    bool operator ==(const any_channel &lhs) const
    {
        if_has_no_value_throw();
        return ptr == lhs.ptr;
    }

    size_t hash() const
    {
        if_has_no_value_throw();
        return std::hash<void *>{}(ptr);
    }

    bool operator <(const any_channel &rhs) const
    {
        return ptr < rhs.ptr;
    }

private:
    void if_has_no_value_throw() const { if (!_ch.has_value()) throw empty_any_channel(); }

    bool ready_nolock() const
    {
        if_has_no_value_throw();
        return _ready();
    }

    bool is_closing_nolock() const
    {
        if_has_no_value_throw();
        return _is_closing();
    }

    bool is_closed_nolock() const
    {
        if_has_no_value_throw();
        return _is_closed();
    }

    std::any take_msg_nolock() const
    {
        if_has_no_value_throw();
        return _take();
    }

    void register_observer_nolock(detail::select_registration &&reg) const
    {
        if_has_no_value_throw();
        _register(std::move(reg));
    }

    // need to store a copy of the channel to prevent destruction
    std::any _ch;
    // need reference to mutex to protect accessor functions
    std::mutex *m;
    // need pointer to channel's state to implement equality and hashing
    void *ptr;

    std::function<void(std::any)> _send;
    std::function<std::any()> _receive;
    std::function<bool()> _ready;
    std::function<bool()> _is_closing;
    std::function<bool()> _is_closed;
    std::function<std::any()> _take;
    std::function<void(detail::select_registration &&)> _register;

    template<class FwdIter> friend FwdIter select(FwdIter, FwdIter);
};

template<class FwdIter>
FwdIter select(FwdIter first, FwdIter last)
{
    auto obs = std::make_shared<detail::select_observer>();

    auto open_channels = std::distance(first, last);

    std::unique_lock<std::mutex> obs_lock(obs->m);

    // first check if any channel is ready while removing any closed channels
    {
        size_t i = 0;
        for (auto ch = first; ch != last; ch++, i++)
        {
            std::unique_lock<std::mutex> ch_lock(*ch->m);

            //lock_cout(cout << "Thread " << this_thread::get_id() << ": checking channel " << i << '\n');

            if (ch->ready_nolock())
            {
                //lock_cout(cout << "Thread " << this_thread::get_id() << ": channel " << i << " ready\n");
                obs->expired = true;
                obs_lock.unlock();
                ch_lock.unlock();
                return ch;
            }
            else if (ch->is_closed_nolock())
            {
                //lock_cout(cout << "Thread " << this_thread::get_id() << ": channel " << i << " closed\n");
                open_channels--;
            }
            else
            {
                //lock_cout(cout << "Thread " << this_thread::get_id() << ": registering observer on channel " << i << '\n');
                ch->register_observer_nolock(detail::select_registration(obs, i));
            }
        }
    }

    if (open_channels == 0)
    {
        obs->expired = true;
        throw all_channels_closed{};
    }

    // check if readied channel is closed when notified
    // if closed then wait for another channel unless all the channels are closed
    for (;;)
    {
        if (open_channels == 0)
        {
            obs->expired = true;
            throw all_channels_closed{};
        }

        //lock_cout(cout << "Thread " << this_thread::get_id() << ": waiting...\n");
        while (!obs->ready)
            obs->cond.wait(obs_lock);
        //lock_cout(cout << "Thread " << this_thread::get_id() << ": awake!\n");

        size_t id = obs->readied_channel;
        //lock_cout(cout << "Thread " << this_thread::get_id() << ": readied channel = " << id << "\n");

        auto ch = first;
        std::advance(ch, id);

        //lock_cout(cout << "Thread " << this_thread::get_id() << ": locking channel " << id << "\n");
        std::unique_lock<std::mutex> ch_lock(*ch->m);

        if (ch->is_closed_nolock())
        {
            //lock_cout(cout << "Thread " << this_thread::get_id() << ": channel " << id << " closed\n");
            open_channels--;
        }
        else
        {
            //lock_cout(cout << "Thread " << this_thread::get_id() << ": channel " << id << " ready\n");
            obs->expired = true;
            obs_lock.unlock();
            ch_lock.unlock();
            return ch;
        }
    }
    //lock_cout(cout << "Thread " << this_thread::get_id() << ": exiting select\n");
}

template<class FwdIter>
std::optional<FwdIter> try_select(FwdIter first, FwdIter last)
{
    auto open_channels = std::distance(first, last);

    // check if any channel is ready while removing any closed channels
    size_t i = 0;
    for (auto ch = first; ch != last; ch++, i++)
    {
        std::unique_lock<std::mutex> ch_lock(*ch->m);

        //lock_cout(cout << "Thread " << this_thread::get_id() << ": checking channel " << i << '\n');

        if (ch->ready_nolock())
        {
            //lock_cout(cout << "Thread " << this_thread::get_id() << ": channel " << i << " ready\n");
            std::any msg = ch->take_msg_nolock();
            ch_lock.unlock();
            return std::make_optional(msg);
        }
        else if (ch->is_closed_nolock())
        {
            //lock_cout(cout << "Thread " << this_thread::get_id() << ": channel " << i << " closed\n");
            open_channels--;
        }
    }

    if (open_channels == 0)
    {
        throw all_channels_closed{};
    }

    // no channel was ready but some were still open
    return {};
}

} // end namespace bierj

template<> struct std::hash<bierj::any_channel>
{
    size_t operator ()(const bierj::any_channel &ch) const noexcept
    {
        return ch.hash();
    }
};

template<> struct std::less<bierj::any_channel>
{
    bool operator ()(const bierj::any_channel &a, const bierj::any_channel &b) const
    {
        return a < b;
    }
};

#endif
