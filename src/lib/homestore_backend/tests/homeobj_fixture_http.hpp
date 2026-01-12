#pragma once

#include <pistache/client.h>
#include <pistache/http.h>
#include <pistache/net.h>

class HttpHelper {
public:
    HttpHelper(const std::string& host, uint16_t port) {
        auto opts = Pistache::Http::Experimental::Client::options().threads(1).maxConnectionsPerHost(8);
        client_.init(opts);
        server_addr_ = "http://" + host + ":" + std::to_string(port);
    }

    ~HttpHelper() { client_.shutdown(); }

    Pistache::Http::Response get(const std::string& resource) {
        auto resp = client_.get(server_addr_ + resource).send();
        Pistache::Http::Response response;
        resp.then([&](Pistache::Http::Response r) { response = r; }, Pistache::Async::Throw);
        Pistache::Async::Barrier< Pistache::Http::Response > barrier(resp);
        barrier.wait();
        return response;
    }

    Pistache::Http::Response post(const std::string& resource, const std::string& body) {
        auto resp = client_.post(server_addr_ + resource).body(body).send();
        Pistache::Http::Response response;
        resp.then([&](Pistache::Http::Response r) { response = r; }, Pistache::Async::Throw);
        Pistache::Async::Barrier< Pistache::Http::Response > barrier(resp);
        barrier.wait();
        return response;
    }

    Pistache::Http::Response del(const std::string& resource, const std::string& body) {
        auto resp = client_.del(server_addr_ + resource).body(body).send();
        Pistache::Http::Response response;
        resp.then([&](Pistache::Http::Response r) { response = r; }, Pistache::Async::Throw);
        Pistache::Async::Barrier< Pistache::Http::Response > barrier(resp);
        barrier.wait();
        return response;
    }

private:
    Pistache::Http::Experimental::Client client_;
    std::string server_addr_;
};
