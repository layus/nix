#include "nix/store/config.hh"

#if NIX_WITH_GRPC

#  include "grpc-common.hh"

namespace nix::grpc_transport {

/**
 * Dumb preshared-token authentication: the client sends the token in the
 * `auth-token` metadata header and the server checks it for equality against
 * its configured token. An empty configured token disables auth.
 */
grpc::Status checkAuth(grpc::ServerContext & ctx, const std::string & token)
{
    if (token.empty())
        return grpc::Status::OK; // auth disabled

    const auto & md = ctx.client_metadata();
    auto it = md.find("auth-token");
    if (it != md.end() && std::string_view(it->second.data(), it->second.size()) == token)
        return grpc::Status::OK;

    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "invalid or missing auth token");
}

} // namespace nix::grpc_transport

#endif // NIX_WITH_GRPC
