#include <sys/mman.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <queue>
#include <stack>
#include <string>
#include <chrono>

#include "/usr/include/x86_64-linux-gnu/sys/time.h"
#include "common.h"
#include <string>

using namespace std;
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
    // better_memcpy(sdmsg + 1, ptr, BLOCKSIZE);
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
    // better_memcpy(msg + 1, src, BLOCKSIZE);
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

static void test(int Type) {
  ofstream OutAllTime("AllTime.txt", ios::app);
  ofstream OutIOPS("IOPS.txt", ios::app);
  ofstream OutLatency("Latency.txt", ios::app);
  ofstream OutThroughput("Throughput.txt", ios::app);
  ofstream OutThroughput4096("Throughput2.txt", ios::app);
  for (int percent = 0; percent <= 100; percent += 10) { // c为正确率
    // 读取索引
    std::fstream fs("index/index" + std::to_string(percent), std::ios::in);
    fs.read(reinterpret_cast<char *>(Index), sizeof(Index));
    fs.close();

    std::cerr << "index" << percent << std::endl;

    for (int l = 1; l <= 1024; l *= 2) {
      // 真正开始测试
      int cs = 2 * BLOCKS_PER_SERVER; // 总的容量大小
      int ops = cs / l;
      auto start = std::chrono::high_resolution_clock::now();
      if (Type == 0) {
        for (int i = 0; i + l <= cs; i += l) { // i是块号
          dbs_write(i, nullptr, l);
        }
      }
      else {
        for (int i = 0; i + l <= cs; i += l) {
          dbs_read(i, nullptr, l);
        }
      }
      auto end = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
      cout << "All Time is " << duration.count() << " us" << endl;
      cout << "IOPS is " << 1000000.0 * ops / duration.count() << endl;
      cout << "Latency is " << 1.0 * duration.count() / ops << " us" << endl;
      cout << "Throughput is " << 1000000.0 * cs * BLOCKSIZE / 1024 / 1024 / 1024 / duration.count() << " GiB/s" << " = " << 1000000.0 * cs * BLOCKSIZE / 1024 / 1024 / duration.count() << " MiB/s" << endl;
      cout << "if use blocksize = 4096, " << "the throughout is  " << 1000000.0 * cs * 4096 / 1024 / 1024 / 1024 / duration.count() << " GiB/s" << " = " << 1000000.0 * cs * 4096 / 1024 / 1024 / duration.count() << " MiB/s" << endl;
      std::flush(std::cout);
      OutAllTime << duration.count() << " ";
      OutIOPS << 1000000.0 * ops / duration.count() << " ";
      OutLatency << 1.0 * duration.count() / ops << " ";
      OutThroughput << 1000000.0 * cs * BLOCKSIZE / 1024 / 1024 / 1024 / duration.count() << " ";
      OutThroughput4096 << 1000000.0 * cs * 4096 / 1024 / 1024 / 1024 / duration.count() << " ";
    }
    std::cerr << std::endl;
    OutAllTime << std::endl;
    OutIOPS << std::endl;
    OutLatency << std::endl;
    OutThroughput << std::endl;
    OutThroughput4096 << std::endl;
  }  
  // std::cerr << avail.size() << std::endl;
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
    std::cerr << addr << " connecting" << std::endl;
    int session = rpc->create_session(addr, 0);
    while (!rpc->is_connected(session)) rpc->run_event_loop_once();
    std::cerr << addr << " connected" << std::endl;
    if (i == 0 || i == 1) serverSess[i] = session;
  }
  fs.close();

  rpc->run_event_loop(10000);
}

int main(int argc, char *argv[]) {
  string configfilename = "config" + std::to_string(atoi(argv[1]));
  hostid = atoi(argv[2]);
  int Type = atoi(argv[3]);
  init(configfilename);
  test(Type);
  delete rpc;
}
