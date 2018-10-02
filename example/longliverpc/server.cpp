// Copyright (c) 2018 Yubin Ruan

#include <map>
#include <pthread.h>
#include <exception>
#include <mutex>
#include <memory>
#include <gflags/gflags.h>
#include <butil/logging.h>
#include <butil/endpoint.h>
#include <brpc/server.h>
#include <brpc/channel.h>
#include <brpc/http_method.h>
#include "longliverpc.pb.h"

DEFINE_string(protocol, "baidu_std", "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)"); 
DEFINE_int32(port, 7777, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_int32(loglevel, 0, "Minimum log level (INFO is 0)");

namespace example {

class HeartBeatServiceImpl;
class ServerCmdServiceImpl;
class RegisterServiceImpl;

class RegisterServiceImpl :  public RegisterService {
private:
    struct Session {
        in_addr_t ip;
        uint32_t port;
        uint64_t timer;
    };
    typedef std::map<int, struct Session> ClientRegistrationMap;

    // this function will be run at background, updating _current_time
    static void *advanceCurrentTime(void *This) {
        // update the timer per 1 second
        while(true) {
            ((RegisterServiceImpl *)This)->_current_time += 1;
            usleep(1000 * 1000);
        }
        return NULL;
    }

    // run at background, removing idle session
    static void *removeSession(void *This) {
        RegisterServiceImpl *obj = (RegisterServiceImpl *)This;
        while (true) {
            for (auto iter = obj->_client_registration.begin();
                 iter != obj->_client_registration.end(); iter++) {
                int idle_time = obj->_current_time - iter->second.timer;
                if (idle_time / 60 > 2) {
                    // remove idle agent session if heart beat is not recieved
                    // for it for more than 2 minutes
                    std::lock_guard<std::mutex> guard(obj->_client_registration_lock);
                    obj->_client_registration.erase(iter);
                    LOG(WARNING) << "Removed idle session of "
                                 << butil::ip2str(butil::int2ip(iter->first)).c_str();
                }
            }
            usleep(1000 * 1000);
        }
        return NULL;
    }

public:
    RegisterServiceImpl() {

        // start background thread
        // Because we use usleep(), we have to use pthread, which use 1-1 thread
        // model, instead of bthread, which use M-N thread model
        if (0 != pthread_create(&_tids[0], NULL, advanceCurrentTime, this)
            || 0 != pthread_create(&_tids[1], NULL, removeSession, this)) {
            throw std::runtime_error("Fail pthread_create()");
        }
        if (0 != pthread_detach(_tids[0]) || 0 != pthread_detach(_tids[1])) {
            throw std::runtime_error("Fail pthread_detach()");
        }
    };
    ~RegisterServiceImpl() {};
    virtual void RegisterClient(google::protobuf::RpcController *cntl_base,
                                const RegisterRequest *request,
                                RegisterResponse *response,
                                google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller*>(cntl_base);
        in_addr_t ip = butil::ip2int(cntl->remote_side().ip);
        std::lock_guard<std::mutex> guard(_client_registration_lock);
        auto iter = _client_registration.find(ip);
        if (iter == _client_registration.end()) {
            struct Session sess {ip, request->port(), _current_time};
            _client_registration.insert(
                        iter,
                        ClientRegistrationMap::value_type(ip, sess));
            LOG(INFO) << "Receive registration from: "
                      << cntl->remote_side();
        } else {
            LOG(WARNING) << "Session already exists for agent<"
                         << cntl->remote_side().ip
                         << ">.";
            // update the time counter of this session
            iter->second.timer = _current_time;
        }
        response->set_status(0);
    }

    virtual int updateClientSessionTimer(in_addr_t ip) {
        int result = -1;
        std::lock_guard<std::mutex> guard(_client_registration_lock);
        auto iter = _client_registration.find(ip);
        if (iter != _client_registration.end()) {
            iter->second.timer = _current_time;
            result = 0;
        } else {
            LOG(WARNING) << "Fail to find session of: "
                         << butil::ip2str(butil::int2ip(ip)).c_str();
            result = -1;
        }
        return result;
    }

private:
    friend class HeartBeatServiceImpl;
    friend class ServerCmdServiceImpl;

    pthread_t _tids[2];
    std::map<in_addr_t, struct Session> _client_registration;
    std::mutex _client_registration_lock;
    std::atomic<uint64_t> _current_time;
};

class HeartBeatServiceImpl : public HeartBeatService {
public:
    HeartBeatServiceImpl(std::shared_ptr<RegisterServiceImpl> registerServicePtr)
        : _registerServiceImplPtr(registerServicePtr) {};
    ~HeartBeatServiceImpl() {};
    virtual void HeartBeat(google::protobuf::RpcController *cntl_base,
                           const HeartBeatRequest *request,
                           HeartBeatResponse *response,
                           google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller*>(cntl_base);
        VLOG(2) << "Received heart beat request from " << cntl->remote_side() 
                  << " to " << cntl->local_side();
        // update timer
        in_addr_t ip = butil::ip2int(cntl->remote_side().ip);
        int res = _registerServiceImplPtr->updateClientSessionTimer(ip);
        if (0 == res) {
            response->set_status(0);
        } else {
            response->set_status(-1);
            cntl->SetFailed("Heart beat failed");
        }
    }

private:
    std::shared_ptr<RegisterServiceImpl> _registerServiceImplPtr;
};

class ServerCmdServiceImpl : public ServerCmdService {
public:
    ServerCmdServiceImpl(std::shared_ptr<RegisterServiceImpl> registerServicePtr)
        : _registerServiceImplPtr(registerServicePtr) {
        // initialize channel option
        _channelOption.protocol = FLAGS_protocol;
        _channelOption.connection_type = FLAGS_connection_type;
        _channelOption.timeout_ms = FLAGS_timeout_ms/*milliseconds*/;
        _channelOption.max_retry = FLAGS_max_retry;
    };
    ~ServerCmdServiceImpl() {};
    virtual void SendCmdToAgent(google::protobuf::RpcController *cntl_base,
                                const ServerCmdRequest *request,
                                ServerCmdResponse *response,
                                google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);
        LOG(INFO) << "request ip: " << request->ip();
        LOG(INFO) << "request cmd: " << request->cmd();
        LOG(INFO) << "request arguments: " << request->arguments();
        LOG(INFO) << "http request content_type: " << cntl->http_request().content_type();
        LOG(INFO) << "http request method: " << brpc::HttpMethod2Str(cntl->http_request().method());
        auto sessionMap = _registerServiceImplPtr->_client_registration;
        butil::ip_t ip;
        if (0 != butil::str2ip(request->ip().c_str(), &ip)) {
            LOG(WARNING) << "Invalid ip address";
            cntl->SetFailed("Invalid ip address");
            response->set_return_code(400);
            response->set_return_string("");
            return;
        }
        in_addr_t ipaddr = butil::ip2int(ip);
        auto iter = sessionMap.find(ipaddr);
        if (iter == sessionMap.end()) {
            LOG(INFO) << "agent not registered yet: " << cntl->remote_side().ip;
            cntl->SetFailed("agent not registered yet.");
            response->set_return_code(400);
            response->set_return_string("");
            return;
        }
        butil::EndPoint ep {ip, (int)iter->second.port};
        brpc::Channel channel;
        if (channel.Init(butil::endpoint2str(ep).c_str(), "", &_channelOption) != 0) {
            LOG(ERROR) << "Fail to init channel";
            cntl->SetFailed("Fail to init channel");
            response->set_return_code(400);
            response->set_return_string("");
            return;
        }

        brpc::Controller cmdCntl;
        example::CmdRequest cmdRequest;
        example::CmdResponse cmdResponse;
        cmdRequest.set_cmd(request->cmd());
        cmdRequest.set_arguments(request->arguments());
        example::AgentService_Stub agentServiceStub(&channel);
        agentServiceStub.RunCmdOnAgent(&cmdCntl, &cmdRequest, &cmdResponse, NULL);
        if (!cmdCntl.Failed()) {
            LOG(INFO) << "Received cmd response from " << cmdCntl.remote_side()
                << " latency=" << cmdCntl.latency_us() << "us";
            response->set_return_code(cmdResponse.return_code());
            response->set_return_string(cmdResponse.return_string());
        } else {
            cntl->SetFailed("fail to run cmd");
            response->set_return_code(400);
            response->set_return_string("");
            return;
        }
    }
private:
    brpc::ChannelOptions _channelOption;
    std::shared_ptr<RegisterServiceImpl> _registerServiceImplPtr;
};
}  // namespace example

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    // log to file
    logging::LoggingSettings settings;
    settings.logging_dest = logging::LOG_TO_FILE;
    if (!logging::InitLogging(settings)) {
        fprintf(stderr, "Fail to initialize logging components");
        return -1;
    }
    logging::SetMinLogLevel(FLAGS_loglevel);

    brpc::Server server;

    example::RegisterServiceImpl registerServiceImpl;
    // TODO why not brace
    // OK:
    auto registerServicePtr =
        std::shared_ptr<example::RegisterServiceImpl>(&registerServiceImpl);
    example::HeartBeatServiceImpl heartBeatServiceImpl(registerServicePtr);
    //
    // not OK:
    // example::HeartBeatServiceImpl heartBeatServiceImpl(
    //     std::shared_ptr<example::RegisterServiceImpl>(&registerServiceImpl)
    // );
    //
    //                 And we also have double free corruption when Ctrl-C
    // example::HeartBeatServiceImpl heartBeatServiceImpl{
    //     std::shared_ptr<example::RegisterServiceImpl>(&registerServiceImpl)
    // };

    example::ServerCmdServiceImpl serverCmdServiceImpl(registerServicePtr);

    // Add the service into server. Notice the second parameter, because the
    // service is put on stack, we don't want server to delete it, otherwise
    // use brpc::SERVER_OWNS_SERVICE.
    if (server.AddService(&registerServiceImpl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service RegisterServiceImpl";
        return -1;
    }
    if (server.AddService(&heartBeatServiceImpl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service HeartBeatServiceImpl";
        return -1;
    }

    if (server.AddService(&serverCmdServiceImpl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE,
                          "/cmd => SendCmdToAgent") != 0) {
        LOG(ERROR) << "Fail to add service ServerCmdServiceImpl";
        return -1;
    }

    // Start the server.
    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start server";
        return -1;
    }

    // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
    server.RunUntilAskedToQuit();
    return 0;
}
