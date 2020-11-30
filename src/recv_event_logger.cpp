#include <string>
#include <chrono>
#include <stdlib.h>
#include <iostream>

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>
#include <spdlog/async.h>

#include "recv_event_logger.hpp"

namespace BPSLogger
{

static size_t start_identifier_size = 3 * sizeof(int) + 4 + sizeof(uint64_t); 
static size_t end_identifier_size = 2*sizeof(int) + 4 + sizeof(uint64_t); 

RecvEventLogger::RecvEventLogger() {
    char* log_path_from_env = std::getenv("ZMQ_RECV_TIMESTAMP_PATH");
    std::string log_path;
    if (log_path_from_env) {
        log_path = log_path_from_env;
    } else {
        log_path = "./zmq_recv_timestamp.log";
    }
    Init("ZMQ_RECV_LOGGER", log_path);
}

RecvEventLogger& RecvEventLogger::GetLogger() {
    static RecvEventLogger recv_event_logger;
    return recv_event_logger;
}

void RecvEventLogger::Init(std::string logger_name, std::string log_path) {
    async_logger_ = spdlog::basic_logger_mt<spdlog::async_factory>(logger_name, log_path);
    spdlog::flush_every(std::chrono::seconds(3));
    async_logger_->set_pattern("%v");
    inited_ = true;
}

void RecvEventLogger::LogEvent(bool is_start, bool is_push, bool is_req, uint64_t tid, int sender, int recver) {
    auto now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch());
    async_logger_->info("{} {} {} {} {} {} {}", std::to_string(now.count()), is_start, is_push, is_req, tid, sender, recver);
}

void RecvEventLogger::LogString(std::string s) {
    async_logger_->info("{}", s);
}

void DeserializeUInt64(uint64_t* integer, char* buf) {
  *integer = 0;
  for(int byte_index=0; byte_index<sizeof(uint64_t); byte_index++) {
    *integer += (unsigned char)buf[byte_index] << byte_index * 8;
  }
}

void DeserializeInt(int* integer, char* buf) {
  *integer = 0;
  for(int byte_index=0; byte_index<sizeof(int); byte_index++) {
    *integer += buf[byte_index] << byte_index * 8;
  }
}

bool ParseAndLogStartIdentifier(char* data) {
    // data format:
    // message data size(int) + 's:' + is_request(char) + is_push(char) + sender(int) + recver(int) + key(uint64)
    if (data[sizeof(int)] != 's' || data[sizeof(int)+1] != ':') return false;
    bool should_return_false = false;
    int message_size;
    DeserializeInt(&message_size, data);
    // if (message_size <= 0) return false;
    if (message_size <= 0) should_return_false = true;

    int is_request = static_cast<int>(data[sizeof(int) + 2]);
    // if (is_request != 0 && is_request != 1) return false;
    if (is_request != 0 && is_request != 1) should_return_false = true;
    int is_push = static_cast<int>(data[sizeof(int) + 3]);
    // if (is_push != 0 && is_push != 1) return false;
    if (is_push != 0 && is_push != 1) should_return_false = true;

    int sender;
    DeserializeInt(&sender, data + sizeof(int) + 4);
    int recver;
    DeserializeInt(&recver, data + 2 * sizeof(int) + 4);
    uint64_t key;
    DeserializeUInt64(&key, data + 3 * sizeof(int) + 4);
    // if (key == UINT64_MAX) return false;
    if (key == UINT64_MAX) should_return_false = true;
    if (key == 13434880ULL) {
        if (should_return_false)
            std::cout << "Encountered key 13434880ULL but returned FALSE!!!!!" << std::endl;
        else
            std::cout << "Encountered key 13434880ULL and logged successfully." << std::endl;
    }
    if (should_return_false) return false;
    RecvEventLogger::GetLogger().LogEvent(true, static_cast<bool>(is_push), static_cast<bool>(is_request), key, sender, recver);
    return true;
}

bool ParseAndLogEndIdentifier(char* data) {
    // data format:
    // 'e:' + is_request(char) + is_push(char) + sender(int) + recver(int) + key(uint64)
    if (data[0] != 'e' || data[1] != ':') return false;

    int is_request = static_cast<int>(data[2]);
    if (is_request != 0 && is_request != 1) return false;
    int is_push = static_cast<int>(data[3]);
    if (is_push != 0 && is_push != 1) return false;

    int sender;
    DeserializeInt(&sender, data + 4);
    int recver;
    DeserializeInt(&recver, data + sizeof(int) + 4);
    uint64_t key;
    DeserializeUInt64(&key, data + 2 * sizeof(int) + 4);
    if (key == UINT64_MAX) return false;

    RecvEventLogger::GetLogger().LogEvent(false, static_cast<bool>(is_push), static_cast<bool>(is_request), key, sender, recver);
    return true;
}

bool ParseAndLogPossibleIdentifier(zmq::msg_t* msg) {
    if (msg->size() == start_identifier_size) {
        // may be a start identifier
        char *data = static_cast<char *>(msg->data());
        return ParseAndLogStartIdentifier(data);
    } else if (msg->size() == end_identifier_size){
        // may be an end identifier
        char *data = static_cast<char *>(msg->data());
        return ParseAndLogEndIdentifier(data);
    } else {
        return false;
    }
}

bool ParseAndLogPossibleRepeatedIdentifier(zmq::msg_t* msg) {
    char *data = static_cast<char *>(msg->data());
    if (data[sizeof(int)] == 's' && data[sizeof(int)+1] == ':') {
        // may be a start identifier
        return ParseAndLogStartIdentifier(data);
    } else if (data[0] == 'e' && data[1] == ':'){
        // may be an end identifier
        return ParseAndLogEndIdentifier(data);
    } else {
        return false;
    }
}

} // namespace BPSLogger