// Copyright (c) Yubin Ruan 2018

#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <array>
#include <gflags/gflags.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include "longliverpc.pb.h"

DEFINE_string(protocol, "baidu_std", "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:7777", "IP Address of server");
DEFINE_int32(port, 9999, "TCP port of every client (as AgentService)");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)"); 

namespace example {

class AgentServiceImpl : public AgentService {
private:
    std::pair<int, std::string> exec(const std::string &cmd) {
        std::array<char, 128> buffer;
        std::string result;
        FILE *pipeStream = popen(cmd.c_str(), "r");
        if (!pipeStream) {
            std::ostringstream ss;
            ss << "error when executing cmd{n"
               << cmd
               << "\n}: popen() failed!";
            return std::make_pair(-1, ss.str());
        }

        while (!feof(pipeStream)) {
            if (fgets(buffer.data(), 128, pipeStream) != nullptr)
                result += buffer.data();
        }

        int status = pclose(pipeStream);
        if (-1 == status) {
            std::ostringstream ss;
            ss << "error when executing cmd{\n"
               << cmd
               <<"\n}: pclose() failed!";
            return std::make_pair(-1, ss.str());
        }
        return std::make_pair(status, result);
    }
public:
    AgentServiceImpl() {};
    ~AgentServiceImpl() {};
    virtual void RunCmdOnAgent(google::protobuf::RpcController *cntl_base,
                               const CmdRequest *request,
                               CmdResponse *response,
                               google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        // brpc::Controller *cntl = static_cast<brpc::Controller*>(cntl_base);

        std::string command = request->cmd() + " " + request->arguments();
        auto result = this->exec(command);
        response->set_return_code(result.first);
        response->set_return_string(result.second);
        LOG(INFO) << "Completed cmd: " << command;
    }
};
} // namespace example

int main(int argc, char *argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Server server;
    example::AgentServiceImpl agentServiceImpl;
    // TODO what does this SERVER_DOESNT_OWN_SERVICE mean exactly ?
    if (server.AddService(&agentServiceImpl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add AgentService";
        return -1;
    }
    // start the server
    brpc::ServerOptions serverOptions;
    if (server.Start(FLAGS_port, &serverOptions) != 0) {
        LOG(ERROR) << "Fail to start AgentService";
        return -1;
    }
    LOG(INFO) << "AgentService started successfully!";

    brpc::Channel channel;
    brpc::ChannelOptions channelOptions;
    channelOptions.protocol = FLAGS_protocol;
    channelOptions.connection_type = FLAGS_connection_type;
    channelOptions.timeout_ms = FLAGS_timeout_ms/*milliseconds*/;
    channelOptions.max_retry = FLAGS_max_retry;
    if (channel.Init(FLAGS_server.c_str(), "", &channelOptions) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }

    // channel can be shared by all threads
    example::RegisterService_Stub registerStub(&channel);
    // synchronously register agent to server
    // server use ip for registration so we make request empty ..
    example::RegisterRequest regRequest;
    regRequest.set_port(FLAGS_port);
    example::RegisterResponse regResponse;
    brpc::Controller registerCntl;
    // Because `done'(last parameter) is NULL, this function waits until
    // the response comes back or error occurs(including timedout).
    registerStub.RegisterClient(&registerCntl, &regRequest, &regResponse, NULL);
    if (!registerCntl.Failed()) {
        LOG(INFO) << "Registration result: received response from server. "
                  << "status: " << regResponse.status()
                  << " latency=" << registerCntl.latency_us() << "us";
    } else {
        LOG(ERROR) << registerCntl.ErrorText();
        return -1;
    }

    example::HeartBeatService_Stub heartBeatStub(&channel);
    while (!brpc::IsAskedToQuit()) {
        example::HeartBeatRequest hbRequest;
        example::HeartBeatResponse hbResponse;
        brpc::Controller hbCntl;
        heartBeatStub.HeartBeat(&hbCntl, &hbRequest, &hbResponse, NULL);
        if (!hbCntl.Failed() && 0 == hbResponse.status()) {
            LOG(INFO) << "Receive heart beat response from server. "
                      << " latency=" << hbCntl.latency_us() << "us";
        } else {
            LOG(WARNING) << "Fail to heart beat. "
                         << "Status: " << hbResponse.status()
                         << " Hint: " << hbCntl.ErrorText();
            // if an agent fail to heart beat for too long server will send
            // message to administrator, so agents dont have to do that.
        }
        // heartbeat per 1 second
        usleep(1000 * 1000L);
    }

    LOG(INFO) << "Client is going to quit";
    return 0;
}
