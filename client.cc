#include <sys/mman.h>

#include <fstream>
#include <iostream>
#include <map>
#include <queue>
#include <stack>
#include <string>

#include "/usr/include/x86_64-linux-gnu/sys/time.h"
#include "common.h"
#include <string>

using std::string;

static int hostid;
static uint64_t hostName;
static uint64_t Index[2 * BLOCKS_PER_SERVER];  // blockID -> serverID
static uint64_t serverSess[10];                // serverID -> session
static std::string hostAddr;

static erpc::Nexus *nexus;
static erpc::MsgBuffer reqs[N];
static erpc::MsgBuffer resps[N];
static std::queue<int64_t> avail;
static erpc::Rpc<erpc::CTransport> *rpc;

int dbs_read(uint64_t, void *, uint64_t);
int dbs_write(uint64_t, void *, uint64_t);

static void cont_func(void *, void *ctx) {
  int k = (int64_t)ctx;
  avail.push(k);
}

static void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

void req_handler(erpc::ReqHandle *req_handle, void *) {
  auto *msg = reinterpret_cast<CommonMsg *>(req_handle->get_req_msgbuf()->buf_);
  if (msg->type != MsgType::CM) exit(-1);

  auto ms = (Mission *)msg->u.CM.ms;
  uint32_t seq = ms->nxt++;
  if (ms->type == MsgType::Write) {
    if (seq >= ms->tot) {
      auto &respbuf = req_handle->pre_resp_msgbuf_;
      rpc->enqueue_response(req_handle, &respbuf);
      return;
    }

    int64_t k = avail.front();
    avail.pop();
    erpc::MsgBuffer &req = reqs[k];
    erpc::MsgBuffer &resp = resps[k];
    CommonMsg *sdmsg = reinterpret_cast<CommonMsg *>(req.buf_);
    sdmsg->type = MsgType::Write;
    auto &ctx = sdmsg->u.Request;
    ctx.bid = ms->bid + seq;
    ctx.ms = (uint64_t)ms;
    void *ptr = (void *)(ms->ptr + seq * BLOCKSIZE);
    better_memcpy(sdmsg + 1, ptr, BLOCKSIZE);
    rpc->resize_msg_buffer(&req, sizeof(CommonMsg) + BLOCKSIZE);
    rpc->enqueue_request(serverSess[Index[ctx.bid]], kReqType, &req, &resp,
                         cont_func, (void *)k);
  }

  else if (ms->type == MsgType::Read) {
    void *dst = (void *)(ms->ptr + (seq - ms->pip) * BLOCKSIZE);
    better_memcpy(dst, msg + 1, BLOCKSIZE);
    if (seq >= ms->tot) {
      auto &respbuf = req_handle->pre_resp_msgbuf_;
      rpc->enqueue_response(req_handle, &respbuf);
      return;
    }

    int64_t k = avail.front();
    avail.pop();
    erpc::MsgBuffer &req = reqs[k];
    erpc::MsgBuffer &resp = resps[k];
    CommonMsg *sdmsg = reinterpret_cast<CommonMsg *>(req.buf_);
    sdmsg->type = MsgType::Read;
    auto &ctx = sdmsg->u.Request;
    ctx.bid = ms->bid + seq;
    ctx.ms = (uint64_t)ms;
    rpc->resize_msg_buffer(&req, sizeof(CommonMsg));
    rpc->enqueue_request(serverSess[Index[ctx.bid]], kReqType, &req, &resp,
                         cont_func, (void *)k);
  }

  auto &respbuf = req_handle->pre_resp_msgbuf_;
  rpc->enqueue_response(req_handle, &respbuf);
}

int dbs_write(uint64_t blockID, void *srcBuf, uint64_t len) { // len是包大小
  // 创建回调任务
  Mission *ms = new Mission;
  ms->type = MsgType::Write;
  ms->tot = len;
  ms->bid = blockID;
  ms->ptr = (uint64_t)srcBuf;
  ms->pip = ms->nxt = std::min(PIPLINE_PKT_DEPTH, len);

  for (int i = 0; i < ms->nxt; i++) {
    int64_t k = avail.front();
    avail.pop();
    erpc::MsgBuffer &req = reqs[k];
    erpc::MsgBuffer &resp = resps[k];
    CommonMsg *msg = reinterpret_cast<CommonMsg *>(req.buf_);
    msg->type = MsgType::Write;
    auto &ctx = msg->u.Request;
    ctx.bid = ms->bid + i;
    ctx.ms = reinterpret_cast<uint64_t>(ms);
    void *src = (void *)((uint64_t)srcBuf + i * BLOCKSIZE);
    better_memcpy(msg + 1, src, BLOCKSIZE);
    rpc->resize_msg_buffer(&req, sizeof(CommonMsg) + BLOCKSIZE);
    rpc->enqueue_request(serverSess[Index[ctx.bid]], kReqType, &req, &resp,
                         cont_func, (void *)k);
  }

  // 数据请求仍实现为阻塞式
  while (ms->nxt < ms->tot + ms->pip) rpc->run_event_loop_once();
  delete ms;
  return 0;
}

int dbs_read(uint64_t blockID, void *dstBuf, uint64_t len) {
  // 创建回调任务
  Mission *ms = new Mission;
  ms->type = MsgType::Read;
  ms->tot = len;
  ms->bid = blockID;
  ms->ptr = (uint64_t)dstBuf;
  ms->pip = ms->nxt = std::min(PIPLINE_PKT_DEPTH, len);

  for (int i = 0; i < ms->nxt; i++) {
    int64_t k = avail.front();
    avail.pop();
    erpc::MsgBuffer &req = reqs[k];
    erpc::MsgBuffer &resp = resps[k];
    CommonMsg *msg = reinterpret_cast<CommonMsg *>(req.buf_);
    msg->type = MsgType::Read;
    auto &ctx = msg->u.Request;
    ctx.bid = ms->bid + i;
    ctx.ms = reinterpret_cast<uint64_t>(ms);
    ctx.ptr = (uint64_t)dstBuf + i * BLOCKSIZE;
    rpc->resize_msg_buffer(&req, sizeof(CommonMsg));
    rpc->enqueue_request(serverSess[Index[ctx.bid]], kReqType, &req, &resp,
                         cont_func, (void *)k);
  }

  // 数据请求仍实现为阻塞式
  while (ms->nxt < ms->tot + ms->pip) rpc->run_event_loop_once();
  delete ms;
  return 0;
}

static char tmp1[1050 * BLOCKSIZE];
static char tmp2[1050 * BLOCKSIZE];

static void test() {
  // 分配大页内存
  uint64_t size = 1024L * 4 * 1024;  // 1024*4KB
  void *ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (ptr == MAP_FAILED) {
    std::cerr << "Failed to allocate memory." << std::endl;
    return;
  }
  uintptr_t address = reinterpret_cast<uintptr_t>(ptr);
  if (address % (4 * 1024) != 0) {
    std::cerr << "Memory not aligned to 4KB." << std::endl;
    return;
  }

  for (int c = 100; c <= 100; c += 10) { // c为正确率
    // 读取索引
    std::fstream fs("index/index" + std::to_string(c), std::ios::in);
    fs.read(reinterpret_cast<char *>(Index), sizeof(Index));
    fs.close();

    std::cerr << "index" << c << std::endl;

    for (int l = 1024; l <= 1024; l *= 2) {
      // 仅仅是做正确性验证
      for (int i = 0; i < BLOCKSIZE * l; i++) 
        tmp1[i] = rand() % 256;
      for (int i = 0; i + l <= 2 * BLOCKS_PER_SERVER;) { // i是块号
        memset(tmp2, 0, BLOCKSIZE * l);
        dbs_write(i, tmp1, l);
        dbs_read(i, tmp2, l);
        // if (memcmp(tmp1, tmp2, BLOCKSIZE * l) != 0) {
        //   std::cerr << "Correctness Test Faild!" << std::endl;
        //   exit(-1);
        // }
        i += l;
      }

      // 真正开始测试
      int cs = 2 * BLOCKS_PER_SERVER; // 总的容量大小
      uint64_t time_usec;
      struct timeval start, end;

      // // std::cerr << "Write Test..." << std::endl;
      gettimeofday(&start, nullptr);
      for (int i = 0; i + l <= cs;) { // i是块号的意思
        dbs_write(i, ptr, l);
        // dbs_read(i % (2 * BLOCKS_PER_SERVER), ptr, l);
        i += l;
      }
      gettimeofday(&end, nullptr);
      time_usec =
          (end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec;
      std::cout << 8.0 * cs * BLOCKSIZE / 1000 / time_usec << " ";
      std::flush(std::cout);
    }
    std::cerr << std::endl;
  }

  assert(avail.size() == N);
  std::cerr << avail.size() << std::endl;
  munmap(ptr, size);
}

static void init(string configname) {
  // 读取本机地址
  std::string addr;
  std::fstream fs(configname, std::ios::in);
  for (int i = 0; i < 3; i++) {
    fs >> addr;
    if (i == hostid) hostAddr = addr, hostName = str2ip_port(addr);
  }
  std::cerr << "hostAddr:" << hostAddr << std::endl;

  nexus = new erpc::Nexus(hostAddr);
  nexus->register_req_func(kReqType, req_handler);
  rpc = new erpc::Rpc<erpc::CTransport>(nexus, nullptr, 0, sm_handler);
  for (int i = 0; i < N; i++) {
    avail.push(i);
    reqs[i] = rpc->alloc_msg_buffer_or_die(MAX_MSG_SIZE);
    resps[i] = rpc->alloc_msg_buffer_or_die(SINGLE_PKT_SIZE);
  }

  // 读取远程地址并建联
  fs.seekg(0, std::ios::beg);
  for (int i = 0; i < 3; i++) {
    fs >> addr;
    if (i == hostid) continue;
    // if (i == 1) continue;
    std::cerr << addr << " connecting" << std::endl;
    int session = rpc->create_session(addr, 0);
    while (!rpc->is_connected(session)) rpc->run_event_loop_once();
    std::cerr << addr << " connected" << std::endl;
    if (i == 0 || i == 1) serverSess[i] = session;
  }
  fs.close();

  rpc->run_event_loop(3000);
}

int main(int argc, char *argv[]) {
  string configfilename = "config" + std::to_string(atoi(argv[1]));
  hostid = atoi(argv[2]);
  init(configfilename);
  test();
  delete rpc;
}
