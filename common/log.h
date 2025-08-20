#pragma once

#include <cstdlib>
#include <iostream>
#include <string>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <cstring>

namespace Log {

enum class LogLevel {
    PANIC,      // Program terminating error (inconsistent state, create coredump, exit immediately)
    FATAL,      // Program terminating error (consistent state, exit immediately)
    ERROR,      // Severe error, but recoverable
    WARNING,    // Potential issues
    INFO,       // General information
    DEBUG,      // Debugging details
};

inline std::string getTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S")
       << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return ss.str();
}

inline const char* logLevelToString(LogLevel level) {
    switch (level) {
        case LogLevel::PANIC:   return "PANIC";
        case LogLevel::FATAL:   return "FATAL";
        case LogLevel::ERROR:   return "ERROR";
        case LogLevel::WARNING: return "WARNING";
        case LogLevel::INFO:    return "INFO";
        case LogLevel::DEBUG:   return "DEBUG";
        default:                return "UNKNOWN";
    }
}

#define ANSI_RESET   "\033[0m"
#define ANSI_RED     "\033[31m"
#define ANSI_YELLOW  "\033[33m"
#define ANSI_GREEN   "\033[32m"
#define ANSI_CYAN    "\033[36m"
#define ANSI_BLUE    "\033[34m"
#define ANSI_MAGENTA "\033[35m"

inline const char* getColorForLevel(LogLevel level) {
    switch (level) {
        case LogLevel::PANIC:   return ANSI_MAGENTA;
        case LogLevel::FATAL:   return ANSI_MAGENTA;
        case LogLevel::ERROR:   return ANSI_RED;
        case LogLevel::WARNING: return ANSI_YELLOW;
        case LogLevel::INFO:    return ANSI_GREEN;
        case LogLevel::DEBUG:   return ANSI_CYAN;
        default:                return ANSI_RESET;
    }
}

inline const char* basename(const char* path) {
    const char* slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

template<typename... Args>
inline void write(LogLevel level, const char* file, int line, const char* format, Args... args) {    
    char buffer[1024];
    snprintf(buffer, sizeof(buffer), format, args...);

    std::cerr << getColorForLevel(level)
              << "[" << getTimestamp() << "] "
              << "[" << logLevelToString(level) << "] "
              << "[" << basename(file) << ":" << line << "] "
              << buffer
              << ANSI_RESET << std::endl;
}

} // namespace Log

#define LOG_PANIC(format, ...) do { \
    Log::write(Log::LogLevel::PANIC, __FILE__, __LINE__, format, ##__VA_ARGS__); \
    ::abort(); \
} while (0)

#define LOG_FATAL(format, ...) do { \
    Log::write(Log::LogLevel::FATAL, __FILE__, __LINE__, format, ##__VA_ARGS__); \
    ::exit(1); \
} while (0)

#define LOG_ERROR(format, ...) \
    Log::write(Log::LogLevel::ERROR, __FILE__, __LINE__, format, ##__VA_ARGS__)

#define LOG_WARNING(format, ...) \
    Log::write(Log::LogLevel::WARNING, __FILE__, __LINE__, format, ##__VA_ARGS__)

#define LOG_INFO(format, ...) \
    Log::write(Log::LogLevel::INFO, __FILE__, __LINE__, format, ##__VA_ARGS__)

#define LOG_DEBUG(format, ...) \
    Log::write(Log::LogLevel::DEBUG, __FILE__, __LINE__, format, ##__VA_ARGS__)
