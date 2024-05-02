#include <cstring>
#include <fstream>
#include <iostream>
using namespace std;
static constexpr uint64_t BLOCKS_PER_SERVER = 500000;

uint64_t Index[BLOCKS_PER_SERVER * 2];
uint64_t WrongIndex[BLOCKS_PER_SERVER * 2];

int main() {
	// 先写全部正确的index
  fstream file("index/index100", ios::out);
  for (int i = 0; i < BLOCKS_PER_SERVER; i++)
    Index[i] = 0, Index[i + BLOCKS_PER_SERVER] = 1;
  file.write((char *)Index, sizeof(Index));
  file.close();

	// 创建各个有错误的index
  for (int c = 0; c <= 90; c += 10) {  // c为index正确率
    file.open("index/index" + to_string(c), ios::out);
    memcpy(WrongIndex, Index, sizeof(Index));
    int wrong = BLOCKS_PER_SERVER * (100 - c) / 100;
    for (int i = 0; i < wrong; i++)
      WrongIndex[i] = 1, WrongIndex[i + BLOCKS_PER_SERVER] = 0;
    file.write((char *)WrongIndex, sizeof(WrongIndex));
    file.close();
  }
	
  return 0;
}