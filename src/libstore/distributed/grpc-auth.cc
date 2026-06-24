#include "nix/store/config.hh"

#if NIX_WITH_GRPC

#  include "grpc-common.hh"

namespace nix::grpc_transport {

/**
 * App/API-key authentication (the first of the pluggable providers from
 * grpc.md). The client sends `authorization: ApiKey <key>`; the server
 * compares against its configured key. mTLS and OAuth2/OIDC providers will be
 * added alongside this, resolving to a principal + scopes; for now a valid key
 * grants full (trusted) access.
 */
grpc::Status checkAuth(grpc::ServerContext & ctx, const std::string & apiKey)
{
    if (apiKey.empty())
        return grpc::Status::OK; // auth disabled

    const auto & md = ctx.client_metadata();
    auto it = md.find("authorization");
    if (it != md.end()) {
        std::string_view got(it->second.data(), it->second.size());
        std::string expected = "ApiKey " + apiKey;
        if (got == expected)
            return grpc::Status::OK;
    }
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "invalid or missing API key");
}

} // namespace nix::grpc_transport

#endif // NIX_WITH_GRPC
