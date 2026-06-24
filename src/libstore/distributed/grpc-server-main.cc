#include "nix/store/config.hh"

#if NIX_WITH_GRPC

#  include "grpc-common.hh"
#  include "nix/store/store-open.hh"
#  include "nix/store/globals.hh"

#  include <cstdlib>
#  include <iostream>

// Minimal launcher for the distributed-store gRPC server. It opens a backing
// store (any store URI, typically a `distributed://...`) and serves it.
//
//   nix-grpc-store-server <listen-addr> <backing-store-uri> [auth-token]
//
// e.g. nix-grpc-store-server 0.0.0.0:5570 \
//        'distributed://?metadata-db-url=postgresql://...&real=/nix/store' SECRET
int main(int argc, char ** argv)
{
    using namespace nix;
    if (argc < 3) {
        std::cerr << "usage: nix-grpc-store-server <listen-addr> <backing-store-uri> [auth-token]\n";
        return 1;
    }
    try {
        initLibStore();
        auto store = openStore(argv[2]);
        grpc_transport::runServer(store, argv[1], argc > 3 ? argv[3] : "");
        return 0;
    } catch (std::exception & e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
}

#else

int main()
{
    return 0;
}

#endif // NIX_WITH_GRPC
