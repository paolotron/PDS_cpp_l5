//
// Created by paolo on 06/06/2021.
//

#ifndef LAB5_COORDINATOR_H
#define LAB5_COORDINATOR_H

using namespace std;
namespace fs = std::filesystem;



template<class MapperInputT, class KeyT,  class ResultT, class AccumulatorT>
class Coordinator {
private:
    function<vector<tuple<KeyT, ResultT>>(const MapperInputT&)> *Mapper=nullptr;
    function<tuple<KeyT, AccumulatorT>(tuple<KeyT&, ResultT&, AccumulatorT&>)> *Reducer=nullptr;

public:
    explicit Coordinator(function<vector<tuple<KeyT, ResultT>>(const MapperInputT&)> *Mapper_p,
                         function<tuple<KeyT, AccumulatorT>(tuple<KeyT&, ResultT&, AccumulatorT&>)> *Reducer_p) {
        this->Mapper = Mapper_p;
        this->Reducer = Reducer_p;
    }

    map<KeyT, AccumulatorT> compute(const string dir, int dim_buff ,int nmappers, int nreducers) {
        shared_ptr<Jobs<MapperInputT>> mapp_in = make_shared<Jobs<MapperInputT>>(dim_buff, 1);
        shared_ptr<Jobs<tuple<KeyT, ResultT>>> red_in = make_shared<Jobs<tuple<KeyT, ResultT>>>(dim_buff, nmappers);
        shared_ptr<map<KeyT, AccumulatorT>> result = make_shared<map<KeyT, AccumulatorT>>();
        std::thread mappers[nmappers];
        std::thread reducers[nreducers];
        std::thread coord(coordinate, dir, mapp_in);
        shared_ptr<std::shared_mutex> red_lock = make_shared<std::shared_mutex>();
        for(auto &i: mappers)
            i = std::thread(Map_t, Mapper, mapp_in, red_in);
        for(auto &i: reducers)
            i = std::thread(Reduce, Reducer, red_in, result, red_lock);
        coord.join();
        for(auto &i: mappers)
            i.join();
        for(auto &i: reducers)
            i.join();
        return *result;
    }

    static void coordinate(const string &path, shared_ptr<Jobs<MapperInputT>> out){
        for (const auto & entry : fs::directory_iterator(path)) {
            ifstream MyReadFile(entry.path());
            string buf;
            while (getline(MyReadFile, buf)) {
                out->put(MapperInputT(buf));
            }
        }
        out->set_end();
    }

    static void Map_t(function<vector<tuple<KeyT, ResultT>>(const MapperInputT&)> *Mapper, shared_ptr<Jobs<MapperInputT>> in, shared_ptr<Jobs<tuple<KeyT, ResultT>>> out){
        optional<MapperInputT> s;
        string buf;
        while((s=in->get())){
            for (auto & MapOut : (*Mapper)(*s))
                out->put(MapOut);
        }
        out->set_end();
    }

    static void Reduce(function<tuple<KeyT, AccumulatorT>(tuple<KeyT&, ResultT&, AccumulatorT&>)> *Reducer,
                shared_ptr<Jobs<tuple<KeyT, ResultT>>> in,
                shared_ptr<map<KeyT, AccumulatorT>> accumulator,
                const shared_ptr<std::shared_mutex>& acc_lock){
        optional<tuple<KeyT, ResultT>> s;
        string buf;
        KeyT key;
        ResultT mapres;
        while((s=in->get())){
            key = get<0>(*s);
            mapres = get<1>(*s);
            acc_lock->lock_shared();
            AccumulatorT firstacc = (*accumulator)[key];
            acc_lock->unlock_shared();
            tuple<KeyT, AccumulatorT> r = (*Reducer)(tuple<KeyT&, ResultT&, AccumulatorT&>(key, mapres, firstacc));
            acc_lock->lock();
            (*accumulator)[get<0>(r)] = get<1>(r);
            acc_lock->unlock();
        }
    }
};


#endif //LAB5_COORDINATOR_H
