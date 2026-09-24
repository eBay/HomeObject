#pragma once

#include <httplib/httplib.h>
#include <chrono>
#include <string>

namespace homeobject {

// Thin synchronous httplib client (formerly Pistache Experimental::Client).
class HttpHelper {
public:
    struct Response {
        int status{0};
        std::string body;

        int code() const { return status; }
        std::string const& body_str() const { return body; }
    };

    HttpHelper(const std::string& host, uint16_t port) : client_{host, static_cast< int >(port)} {
        client_.set_connection_timeout(5, 0);
        client_.set_read_timeout(30, 0);
    }

    ~HttpHelper() = default;

    Response get(const std::string& resource) {
        Response response;
        auto res = client_.Get(resource);
        if (res) {
            response.status = res->status;
            response.body = res->body;
        }
        return response;
    }

    Response post(const std::string& resource, const std::string& body) {
        Response response;
        auto res = client_.Post(resource, body, "application/json");
        if (res) {
            response.status = res->status;
            response.body = res->body;
        }
        return response;
    }

    Response del(const std::string& resource, const std::string& body) {
        Response response;
        auto res = client_.Delete(resource, body, "application/json");
        if (res) {
            response.status = res->status;
            response.body = res->body;
        }
        return response;
    }

private:
    httplib::Client client_;
};

} // namespace homeobject
