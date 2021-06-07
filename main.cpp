#include <iostream>
#include "jobs.h"
#include <memory>
#include <fstream>
#include <filesystem>
#include <regex>
#include <thread>
#include <cassert>
#include "Coordinator.h"

#define NLine 100
#define NFile 40
#define DIM 10
#define EX1 0
namespace fs = std::filesystem;

std::mutex cout_lock;
int tot_end=0;

using namespace std;
void producer(string &directory, const shared_ptr<Jobs<string>>& jobs){
    string buf;
    const std::string& path = directory;
    for (const auto & entry : fs::directory_iterator(path))
        jobs->put(entry.path());
    jobs->set_end();
}

void prod_cons(const shared_ptr<Jobs<string>>& files, const shared_ptr<Jobs<string>>& lines){
    optional<string> s;
    string buf;
    while((s=files->get())){
        ifstream MyReadFile(*s);
        while(getline(MyReadFile, buf)){
            lines->put(buf.substr(0, buf.length()-1));
        }
    }
    lines->set_end();
    lock_guard<mutex> l(cout_lock);
    tot_end++;
    cout << "prod_cons_end:"<< tot_end << '\n';
}

void consumer(string &reg, const shared_ptr<Jobs<string>>& jobs){
    optional<string> s;
    while((s=jobs->get()))
        if(regex_match(*s, regex(reg))) {
            lock_guard<mutex> l(cout_lock);
        }
}

void test_circular(){
        Circular<int> buf(4);
        buf.push(2);
        assert(buf.front() == 2);
        buf.pop();
        assert(buf.empty());
        buf.push(1);
        buf.push(1);
        buf.push(1);
        buf.push(1);
        assert(buf.full());
}

#if EX1
int main(int argc, char** argv) {
    test_circular();
    if(argc != 3)
        return -1;
    string dir = string(argv[1]);
    string reg = string(argv[2]);
    int dim = DIM;
    shared_ptr<Jobs<string>> buffer_dir = make_shared<Jobs<string>>(dim, 1);
    shared_ptr<Jobs<string>> buffer_line = make_shared<Jobs<string>>(dim, NFile);
    std::thread tf[NFile];
    std::thread tc[NLine];
    std::thread tp(producer, ref(dir), ref(buffer_dir));
    for (auto & i : tf)
        i = std::thread(prod_cons, ref(buffer_dir), ref(buffer_line));
    for (auto & i : tc)
        i = std::thread(consumer, ref(reg), ref(buffer_line));
    tp.join();
    for (auto & i : tf)
        i.join();
    for (auto & i : tc)
        i.join();
    return 0;
}
#else

void print_map(map<string, int> &m){
    for(auto el: m)
        cout << get<0>(el) << ": " << get<1>(el) << '\n';
}

int main(){
    auto map_fun = function<vector<tuple<string, int>>(const string&)> ([](const string& is)->vector<tuple<string, int>>{
        vector<tuple<string, int>> res;
        res.emplace_back(is.substr(0, is.find(' ')), 1);
        return res;
    });
    auto red_fun = function<tuple<string, int>(tuple<string&, int&, int&>)>  ([](tuple<string, int, int> in)->tuple<string, int>{
        return tuple<string, int>(get<0>(in), get<1>(in)+get<2>(in));
    });
    Coordinator<string, string, int, int> coord(map_fun, red_fun);
    auto res = coord.compute("../mapred", 100, 3, 3);
    print_map(res);
}



#endif