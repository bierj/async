#pragma once

#ifndef BIERJ_EXECUTORS_HPP
#define BIERJ_EXECUTORS_HPP

#include <bierj/channels.hpp>
#include <exception>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace bierj
{
    class already_joined : public std::exception {};

    template<class Pool>
    class base_threadpool
    {
    public:
        ~base_threadpool()
        {
            if (!joined) std::terminate();
        }

        template<class Fn>
        void send(Fn &&f)
        {
            {
                std::lock_guard<std::mutex> lock(m);
                if (joined)
                {
                    pool->work_chan.reopen();
                    start();
                }
            }
            pool->work_chan.send(move(f));
        }

        void join()
        {
            std::lock_guard<std::mutex> lock(m);
            if (joined) throw already_joined{};
            pool->work_chan.close();
            for (auto &worker : workers) worker.join();
            workers.clear();
            joined = true;
        }

        void joinable() const { std::lock_guard<std::mutex> lock(m); return !joined; }

    protected:
        base_threadpool(Pool *_pool, int _pool_size) : pool(_pool), pool_size(_pool_size) {}

        void start()
        {
            for (int i = 0; i < pool_size; i++)
                workers.emplace_back([this] { worker_loop(); });
            joined = false;
        }

        void worker_loop()
        {
            try
            {
                for (;;)
                {
                    auto [f] = pool->work_chan.receive();
                    f();
                }
            }
            catch (channel_closed) {}
        }

        mutable std::mutex m;
        Pool *pool;
        std::vector<std::thread> workers;
        int pool_size;
        bool joined;
    };

    class threadpool : public base_threadpool<threadpool>
    {
    public:
        threadpool(int pool_size = std::thread::hardware_concurrency()) : base_threadpool(this, pool_size)
        {
            // need to delay start until work_chan has been initialized
            start();
        }

    private:
        channel<std::function<void()>> work_chan;

        template<class Pool> friend class base_threadpool;
    };

    class buffered_threadpool : public base_threadpool<buffered_threadpool>
    {
    public:
        buffered_threadpool(int pool_size = std::thread::hardware_concurrency(), size_t capacity = 0)
            : base_threadpool(this, pool_size), work_chan(capacity)
        {
            // need to delay start until work_chan has been initialized
            start();
        }

    private:
        buffered_channel<std::function<void()>> work_chan;

        template<class Pool> friend class base_threadpool;
    };
}

#endif