#include "JoinQuery.hpp"
#include <assert.h>
#include <thread>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <string>
#include <sstream>
#include<thread>
#include <charconv>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <algorithm>

using namespace std;


size_t ToInt(const char* str)
{
    size_t val = 0;
    while( *str ) {
        val = val*10 + (*str++ - '0');
    }
    return val;
}

static constexpr uint64_t broadcast(uint8_t c) {
   return (static_cast<uint64_t>(c) << 0) | (static_cast<uint64_t>(c) << 8) | (static_cast<uint64_t>(c) << 16) | (static_cast<uint64_t>(c) << 24) | (static_cast<uint64_t>(c) << 32) | (static_cast<uint64_t>(c) << 40) | (static_cast<uint64_t>(c) << 48) | (static_cast<uint64_t>(c) << 56);
}

static const char* findNextLine(const char* iter, const char* limit) {
   auto limit16 = limit - 16;
   while (iter < limit16) {
      auto block = _mm_loadu_si128(reinterpret_cast<const __m128i*>(iter));
      auto pattern = _mm_set1_epi8 ('\n');
      auto hits = _mm_movemask_epi8(_mm_cmpeq_epi8(block, pattern));
      if (hits) 
      {
        return iter + (__builtin_ctzll(hits));
      } else 
      {
        iter+=8;
      }
   }
   while (iter<limit) {
      if ((*iter++)=='\n')
        break;
   }
   return iter;
}

void readCustomersFile(string path, unordered_map<string,vector<string>>& cusHashTable){
    MemoryMappedFile customersFile(path.c_str());

    for (auto iter = customersFile.begin(), limit = customersFile.end();iter<limit;) {
        unsigned column = 1;
        string seg = "";
        string custkey = "";
        for (auto last=iter;;++iter) {
            if ((*iter)=='|') {
                if(column == 1){
                    custkey.assign(last, iter);
                    custkey.erase(remove(custkey.begin(), custkey.end(), '\n'), custkey.cend());
                }
                else if (column == 7) {
                    seg.assign(last, iter);
                    cusHashTable[seg].push_back(custkey);
                    iter = findNextLine(iter, limit);
                    break;
                } else {
                    last=iter+1;
                }
                column++;
            }
        }
    }
}

void readOrdersFile(string path, unordered_map<string,vector<string>>& orderHashTable){
    MemoryMappedFile ordersFile(path.c_str());
    
    for (auto iter = ordersFile.begin(), limit = ordersFile.end();iter<limit;) {
        unsigned column = 1;
        string orderkey = "";
        string custkey = "";
        for (auto last=iter;;++iter) {
            if ((*iter)=='|') {
                if(column == 1){
                    orderkey.assign(last, iter);
                    orderkey.erase(remove(orderkey.begin(), orderkey.end(), '\n'), orderkey.cend());
                    last=iter+1;
                }
                else if (column == 2) {
                    custkey.assign(last, iter);
                    custkey.erase(remove(custkey.begin(), custkey.end(), '\n'), custkey.cend());
                    orderHashTable[custkey].push_back(orderkey);
                    iter = findNextLine(iter, limit);
                    break;
                }
                column++;
            }
        }
    }
}

void readItemsFile(string path, unordered_map<string,pair<double, size_t>>& itemHashTable){
    MemoryMappedFile lineItemsFile(path.c_str());

    for (auto iter = lineItemsFile.begin(), limit = lineItemsFile.end();iter<limit;) {
        unsigned column = 1;
        string orderkey = "";
        string price = "";
        for (auto last=iter;;++iter) {
            if ((*iter)=='|') {
                if(column == 1){
                    orderkey.assign(last, iter);
                    orderkey.erase(remove(orderkey.begin(), orderkey.end(), '\n'), orderkey.cend());
                }
                else if (column == 5) {
                    price.assign(last, iter);
                    price.erase(remove(price.begin(), price.end(), '\n'), price.cend());
                    itemHashTable[orderkey].first += ToInt(price.c_str());
                    itemHashTable[orderkey].second++;
                    iter = findNextLine(iter, limit);
                    break;
                }else{
                    last=iter+1;
                }
                column++;
            }
        }
    }
}


JoinQuery::JoinQuery(string lineitem, string order,string customer){
    customerFileThread = thread{readCustomersFile, customer, ref(cusHashTable)};
    orderFileThread = thread{readOrdersFile, order, ref(orderHashTable)};
    itemFileThread = thread{readItemsFile, lineitem, ref(itemHashTable)};

    customerFileThread.join();  
    orderFileThread.join();
    itemFileThread.join();
}

void Sum(vector<string> custKeys,unordered_map<string,vector<string>> orderHashTable,unordered_map<string,pair<double, size_t>> itemHashTable, int begin, int end,double& sum, size_t& count)
{
    for (int i = begin;i<end; i++)
    {
        for(string orderId: orderHashTable[custKeys[i]]){
            auto p = itemHashTable[orderId];
            sum += p.first;
            count += p.second;
        }
    }
}

size_t JoinQuery::avg(string segmentParam) {
    size_t count = 0;
    double sum = 0;

    
    vector<string> custKeys = cusHashTable[segmentParam];

    int size = custKeys.size()/4;
    
    

    thread t1{Sum, custKeys, orderHashTable,itemHashTable, 0, size, ref(sum), ref(count)};
    thread t2{Sum, custKeys, orderHashTable,itemHashTable, size, 2*size, ref(sum), ref(count)};
    thread t3{Sum, custKeys, orderHashTable,itemHashTable, 2*size, 3*size, ref(sum), ref(count)};
    thread t4{Sum, custKeys, orderHashTable,itemHashTable, 3*size, custKeys.size(), ref(sum), ref(count)};

    t1.join();
    t2.join();
    t3.join();
    t4.join();

    return (sum /count) * 100;
}

size_t JoinQuery::lineCount(std::string rel) {
   ifstream relation(rel);
   assert(relation);
   size_t n = 0;
   for (string line; getline(relation, line);) n++;
   return n;
}
