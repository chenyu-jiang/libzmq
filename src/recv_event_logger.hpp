#ifndef __ZMQ_RECV_EVENT_LOGGER_H_INCLUDED__
#define __ZMQ_RECV_EVENT_LOGGER_H_INCLUDED__

#include <string>
#include <chrono>
#include <stdlib.h>

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>
#include <spdlog/async.h>

#include "msg.hpp"

namespace BPSLogger
{

class RecvEventLogger {
public:
    RecvEventLogger();
    static RecvEventLogger& GetLogger();

    RecvEventLogger(RecvEventLogger const&) = delete;             // Copy construct
    RecvEventLogger(RecvEventLogger&&) = delete;                  // Move construct
    RecvEventLogger& operator=(RecvEventLogger const&) = delete;  // Copy assign
    RecvEventLogger& operator=(RecvEventLogger &&) = delete;      // Move assign

    void Init(std::string logger_name, std::string log_path);

    void LogEvent(bool is_start, bool is_push, bool is_req, uint64_t tid, int sender, int recver);

    void LogString(std::string s);

private:
    std::shared_ptr<spdlog::logger> async_logger_ = nullptr;
    bool inited_ = false;
};

void DeserializeUInt64(uint64_t* integer, char* buf);
void DeserializeInt(int* integer, char* buf);
bool ParseAndLogStartIdentifier(char* data);
bool ParseAndLogEndIdentifier(char* data);
bool ParseAndLogPossibleIdentifier(zmq::msg_t* msg);
bool ParseAndLogPossibleRepeatedIdentifier(zmq::msg_t* msg);
} // namespace BPSLogger

#endif