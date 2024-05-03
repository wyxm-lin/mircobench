#include <sys/mman.h>

#include <fstream>
#include <iostream>
#include <map>
#include <queue>
#include <stack>
#include <string>

#include "common.h"
using std::string;

static int hostid;
static char *buffer = nullptr;
static uint64_t hostName;
static uint64_t clientSess;
static uint64_t serverSess;
static uint64_t Index[2 * BLOCKS_PER_SERVER];  // blockID -> serverID
static std::string hostAddr, clientAddr, serverAddr;

static erpc::Nexus *nexus;
static erpc::MsgBuffer reqs[N];
static erpc::MsgBuffer resps[N];
static std::queue<int64_t> avail;
static erpc::Rpc<erpc::CTransport> *rpc;

static void cont_func(void *, void *cbctx) {
  int64_t kk = (int64_t)cbctx;
  avail.push(kk);
}

static void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

void req_handler(erpc::ReqHandle *req_handle, void *) {
  auto *msg = reinterpret_cast<CommonMsg *>(req_handle->get_req_msgbuf()->buf_);
  auto &ctx = msg->u.Request;
  uint64_t blockID = ctx.bid;

  if (Index[blockID] == hostid) {
    blockID %= BLOCKS_PER_SERVER;

    void *ptr = buffer + blockID * BLOCKSIZE;
    if (msg->type == MsgType::Read) {
      int64_t k = avail.front();
      avail.pop();
      erpc::MsgBuffer &req = reqs[k];
      erpc::MsgBuffer &resp = resps[k];
      CommonMsg *msg = reinterpret_cast<CommonMsg *>(req.buf_);
      msg->type = MsgType::CM;
      msg->u.CM.ms = ctx.ms;
      better_memcpy(msg + 1, ptr, BLOCKSIZE);
      rpc->resize_msg_buffer(&req, sizeof(CommonMsg) + BLOCKSIZE);
      rpc->enqueue_request(clientSess, kReqType, &req, &resp, cont_func,
                           (void *)k);
    }

    else if (msg->type == MsgType::Write) {
      better_memcpy(ptr, msg + 1, BLOCKSIZE);
      int64_t k = avail.front();
      avail.pop();
      erpc::MsgBuffer &req = reqs[k];
      erpc::MsgBuffer &resp = resps[k];
      CommonMsg *msg = reinterpret_cast<CommonMsg *>(req.buf_);
      msg->type = MsgType::CM;
      msg->u.CM.ms = ctx.ms;
      rpc->resize_msg_buffer(&req, sizeof(CommonMsg));
      rpc->enqueue_request(clientSess, kReqType, &req, &resp, cont_func,
                           (void *)k);
    }
  }

  else {
    int64_t k = avail.front();
    avail.pop();
    erpc::MsgBuffer &req = reqs[k];
    erpc::MsgBuffer &resp = resps[k];
    int64_t msg_size = req_handle->get_req_msgbuf()->get_data_size();
    memcpy(req.buf_, msg, msg_size);
    rpc->resize_msg_buffer(&req, msg_size);
    rpc->enqueue_request(serverSess, kReqType, &req, &resp, cont_func,
                         (void *)k);
  }

  auto &resp = req_handle->pre_resp_msgbuf_;
  rpc->enqueue_response(req_handle, &resp);
}

static void init(string configname) {
  // 读取索引
  std::fstream fs("index/index100", std::ios::in);
  fs.read(reinterpret_cast<char *>(Index), sizeof(Index));
  fs.close();

  // 读取本机地址
  std::string addr;
  fs.open(configname.c_str(), std::ios::in);
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
    if (i == 0) {
      serverAddr = addr;
      std::cerr << serverAddr << " connecting" << std::endl;
      serverSess = rpc->create_session(serverAddr, 0);
      while (!rpc->is_connected(serverSess)) rpc->run_event_loop_once();
      std::cerr << serverAddr << " connected" << std::endl;
    }
    if (i == 1) serverAddr = addr;
    if (i == 2) clientAddr = addr;
  }

  if (hostid == 0) {
    rpc->run_event_loop(10000);
    // rpc->run_event_loop(3000);
    std::cerr << serverAddr << " connecting" << std::endl;
    serverSess = rpc->create_session(serverAddr, 0);
    while (!rpc->is_connected(serverSess)) rpc->run_event_loop_once();
    std::cerr << serverAddr << " connected" << std::endl;
  }

  rpc->run_event_loop(10000);
  // rpc->run_event_loop(3000);
  std::cerr << clientAddr << " connecting" << std::endl;
  clientSess = rpc->create_session(clientAddr, 0);
  while (!rpc->is_connected(clientSess)) rpc->run_event_loop_once();
  std::cerr << clientAddr << " connected" << std::endl;
  fs.close();

  rpc->run_event_loop(1000000000);
}

int main(int argc, char *argv[]) {
  string configfilename = "config" + std::to_string(atoi(argv[1]));
  hostid = atoi(argv[2]);
  init(configfilename);
  delete rpc;
}
