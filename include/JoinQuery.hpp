#include<iostream>
#include<fstream>
#include<string>
#include<vector>
#include<unordered_map>
#include <thread>
#include <sys/mman.h>
#include <unistd.h>
#include <immintrin.h>
#include <emmintrin.h>
#include <charconv>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


using namespace std;

class MemoryMappedFile {
   int handle;
   void* mapping;
   size_t size;

   public:
   explicit MemoryMappedFile(const char* fileName) {
      handle = ::open(fileName, O_RDONLY);
      lseek(handle, 0, SEEK_END);
      size = lseek(handle, 0, SEEK_CUR);
      mapping=mmap(nullptr, size, PROT_READ, MAP_SHARED, handle, 0);
   }

   ~MemoryMappedFile() {
      munmap(mapping, size);
      close(handle);
   }
   const char* begin() const { return static_cast<char*>(mapping); }
   const char* end() const { return static_cast<char*>(mapping) + size; }
};


class JoinQuery
{
    private:
    unordered_map<string,vector<string>> cusHashTable;
    unordered_map<string,vector<string>> orderHashTable;
    unordered_map<string,pair<double, size_t>> itemHashTable;
    thread customerFileThread;
    thread orderFileThread;
    thread itemFileThread;

    public:
    JoinQuery(std::string lineitem, std::string orders, std::string customer);
    size_t avg(std::string segmentParam);
    size_t lineCount(std::string rel);
};
