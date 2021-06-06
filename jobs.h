//
// Created by paolo on 05/06/2021.
//

#ifndef LAB5_JOBS_H
#define LAB5_JOBS_H

#include <queue>
#include <vector>
#include <optional>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include "Circular.h"
template <class T>
class Jobs{
private:
    bool isend = false;
    Circular<T> buffer;
    std::mutex lock;
    std::condition_variable cv;
    std::condition_variable full;
    int npr;
    int n_ended;
public:
    explicit Jobs(int dim, int n_prod) {
        buffer = Circular<T>(dim);
        npr = n_prod;
        n_ended=0;
    }

    void put(T job){
        std::unique_lock<std::mutex> ul(lock);
        if(buffer.full())
            full.wait(ul, [&](){return !buffer.full();});
        buffer.push(job);
        cv.notify_one();
    }

    void set_end(){
        std::lock_guard<std::mutex> ul(lock);
        n_ended++;
        if(n_ended == npr){
            this->isend = true;
            cv.notify_all();
        }
    }

    std::optional<T> get(){
        std::unique_lock<std::mutex> ul(lock);
        if(buffer.empty() && isend) {
            return std::nullopt;
        }
        if(buffer.empty()) {
            cv.wait(ul, [&](){return !buffer.empty() || isend;});
            if(isend && buffer.empty())
                return std::nullopt;
        }
        T el = buffer.front();
        buffer.pop();
        full.notify_one();
        return std::optional(el);
    }
};

#endif //LAB5_JOBS_H
