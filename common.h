// #include <immintrin.h>
// #include <stddef.h>
#include <stdio.h>
#include <cstdarg>
#include <iostream>
#include <cassert>
#include "rpc.h"

enum class NodeType : uint8_t { Client = 0, Server, Master };  // 结点类型
enum class MsgType : uint8_t {
  CM = 0,
  Read,
  Write,
  Wrong,
};  // 信息类型

// 通用信息传递结构体
struct CommonMsg {
  MsgType type;  // 信息类型
  union {
    struct {
      uint64_t ms;  // 任务号
    } CM;

    struct {
      uint64_t bid;
      uint64_t ptr;
      uint64_t ms;  // 任务号
    } Request;
  } u;
};

void *memcpy_avx256(void *, const void *, size_t);
void *memcpy_avx512(void *, const void *, size_t);
void *memcpy_null(void *, const void *, size_t) { return nullptr; }

static constexpr uint64_t N = 50;
static constexpr uint8_t kReqType = 2;
// static constexpr uint64_t BLOCKSIZE = 0x400000;
// static constexpr uint64_t BLOCKSIZE = 0x1000;
static constexpr uint64_t BLOCKSIZE = 3824 - sizeof(CommonMsg);
static constexpr uint64_t PIPLINE_PKT_DEPTH = 32;
static constexpr uint64_t SINGLE_PKT_SIZE = 3824;
static constexpr uint64_t MAX_MSG_SIZE = 8353520;
static constexpr uint64_t BLOCKS_PER_SERVER = 500000;
// static constexpr uint64_t PIPLINE_PKT_SIZE = (4 * 3824 - sizeof(CommonMsg));
// static constexpr uint64_t PIPLINE_PKT_NUM =
//     (BLOCKSIZE + PIPLINE_PKT_SIZE - 1) / PIPLINE_PKT_SIZE;
static constexpr void *(*better_memcpy)(void *, const void *, size_t) = memcpy_null;

// 任务信息结构体
struct Mission {
  MsgType type;
  int64_t bid;
  int64_t nxt;
  int64_t tot;
  int64_t pip;
  uint64_t ptr;
};

// 将uint32_t类型的ip地址转换为字符串
std::string ip2str(uint32_t ip) {
  char buf[16];
  snprintf(buf, sizeof(buf), "%d.%d.%d.%d", (ip >> 24) & 0xff,
           (ip >> 16) & 0xff, (ip >> 8) & 0xff, ip & 0xff);
  return std::string(buf);
}

// 将字符串类型的ip地址转换为uint32_t
uint32_t str2ip(const std::string &ip) {
  uint32_t ret = 0;
  uint32_t index = 256 * 256 * 256, tmp = 0;
  for (auto &c : ip) {
    if (c == '.') {
      assert(tmp < 256);
      ret += tmp * index;
      tmp = 0;
      index /= 256;
      continue;
    }
    tmp = tmp * 10 + c - '0';
  }
  assert(index == 1);
  ret += tmp * index;
  return ret;
}

uint64_t str2ip_port(const std::string &ip_port) {
  std::string ip = ip_port.substr(0, ip_port.find(':'));
  uint16_t port = std::stoi(ip_port.substr(ip_port.find(':') + 1));
  return ((uint64_t)str2ip(ip) << 16) | port;
}

std::string ip_port2str(uint64_t ip_port) {
  return ip2str(ip_port >> 16) + ":" + std::to_string(ip_port & 0xffff);
}

void mylog(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  vprintf(fmt, args);
  va_end(args);
}