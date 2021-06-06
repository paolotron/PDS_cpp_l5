//
// Created by paolo on 06/06/2021.
//

#ifndef LAB5_CIRCULAR_H
#define LAB5_CIRCULAR_H
template <class T>
class Circular{
private:
    std::vector<T> buffer;
    int first{};
    int last{};
    int dimension{};
    int count{};
    int next(int i){
        return (i + 1) % dimension;
    }
public:
    Circular()=default;
    explicit Circular(int dim){
        dimension = dim;
        first = last = count = 0;
        buffer = std::vector<T>(dim);
    }
    void push(T element){
        buffer[last] = element;
        last = next(last);
        count++;
    }
    T front(){
        return buffer[first];
    }
    void pop(){
        buffer[first] = T();
        first = next(first);
        count--;
    }
    bool full(){
        return count == dimension;
    }
    bool empty(){
        return count == 0;
    }

};
#endif //LAB5_CIRCULAR_H
