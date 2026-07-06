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
//   nix-grpc-store-server <listen-addr> <backing-store-uri> [auth-token] [advertise-addr]
//
// e.g. nix-grpc-store-server 0.0.0.0:5570 \
//        'distributed://?metadata-db-url=postgresql://...&real=/cluster/store' SECRET 10.0.0.1:5570
//
// If `advertise-addr` is given, the node registers it in the share's
// `var/replicas/<hostname>` registry so that clients can discover the cluster
// (`grpc://?registry=...`). It must be an address clients can reach. Requires
// a `distributed://` backing store (the registry lives on its share);
// daemon-backed servers must not pass it.
int main(int argc, char ** argv)
{
    using namespace nix;
    if (argc < 3) {
        std::cerr << "usage: nix-grpc-store-server <listen-addr> <backing-store-uri> [auth-token] [advertise-addr]\n";
        return 1;
    }
    try {
        initLibStore();
        auto store = openStore(argv[2]);
        grpc_transport::runServer(store, argv[1], argc > 3 ? argv[3] : "", argc > 4 ? argv[4] : "");
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
