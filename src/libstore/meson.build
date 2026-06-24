project(
  'nix-store',
  'cpp',
  version : files('.version'),
  default_options : [
    'cpp_std=c++23',
    # TODO(Qyriad): increase the warning level
    'warning_level=1',
    'errorlogs=true', # Please print logs for tests that fail
    'localstatedir=/nix/var',
  ],
  meson_version : '>= 1.8',
  license : 'LGPL-2.1-or-later',
)

cxx = meson.get_compiler('cpp')

subdir('nix-meson-build-support/deps-lists')

configdata_pub = configuration_data()
configdata_priv = configuration_data()

# TODO rename, because it will conflict with downstream projects
configdata_priv.set_quoted('PACKAGE_VERSION', meson.project_version())

subdir('nix-meson-build-support/default-system-cpu')

# Used in public header.
configdata_pub.set_quoted(
  'NIX_LOCAL_SYSTEM',
  nix_system_cpu + '-' + host_machine.system(),
  description : 'This is the system name Nix expects for local running instance of Nix.\n\n' + 'See the "system" setting for additional details',
)

deps_private_maybe_subproject = []
deps_public_maybe_subproject = [
  dependency('nix-util'),
]
subdir('nix-meson-build-support/subprojects')

can_link_symlink = false
native_ln = find_program('ln', required : false, native : true)
if native_ln.found()
  run_command(
    native_ln,
    '-s',
    meson.project_build_root() / '__nothing_link_target',
    meson.project_build_root() / '__nothing_symlink',
    # native doesn't allow dangling symlinks, which the tests require
    env : {'MSYS' : 'winsymlinks:lnk'},
    check : true,
  )
  can_link_symlink = run_command(
    native_ln,
    meson.project_build_root() / '__nothing_symlink',
    meson.project_build_root() / '__nothing_hardlink',
    check : false,
  ).returncode() == 0
  run_command(
    'rm',
    '-f',
    meson.project_build_root() / '__nothing_symlink',
    meson.project_build_root() / '__nothing_hardlink',
    check : true,
  )
endif
summary('can hardlink to symlink', can_link_symlink, bool_yn : true)
configdata_priv.set('CAN_LINK_SYMLINK', can_link_symlink.to_int())

# Check for each of these functions, and create a define like `#define HAVE_POSIX_FALLOCATE 1`.
check_funcs = [
  'posix_fallocate',
  'statvfs',
  'open_tree',
  'move_mount',
]
foreach funcspec : check_funcs
  define_name = 'HAVE_' + funcspec.underscorify().to_upper()
  define_value = cxx.has_function(funcspec).to_int()
  configdata_priv.set(define_name, define_value)
endforeach

if host_machine.system() == 'linux'
  has_landlock = cxx.has_header('linux/landlock.h')
  configdata_priv.set('HAVE_LANDLOCK', has_landlock.to_int())
endif

has_acl_support = cxx.has_header('sys/xattr.h') and cxx.has_function(
  'llistxattr',
) and cxx.has_function(
  'lremovexattr',
)
# Used in public header. Affects ABI!
configdata_pub.set(
  'NIX_SUPPORT_ACL',
  has_acl_support.to_int(),
  description : 'FIXME: It\'s a bit peculiar that this needs to be exposed. The reason is that that it effects whether the settings struct in a header has a particular field. This is also odd, because it means when there is no ACL support one will just get an "unknown setting" warning from their configuration.',
)

if host_machine.system() == 'darwin'
  sandbox = cxx.find_library('sandbox')
  deps_other += [ sandbox ]
endif

if host_machine.system() == 'windows'
  wsock32 = cxx.find_library('wsock32')
  deps_other += [ wsock32 ]
endif

subdir('nix-meson-build-support/libatomic')

boost = dependency(
  'boost',
  modules : [
    'container',
    # Shouldn't list, because can header-only, and Meson currently looks for libs
    #'regex',
    'url',
  ],
  include_type : 'system',
)
# boost is a public dependency, but not a pkg-config dependency unfortunately, so we
# put in `deps_other`.
deps_other += boost

# This is quite new, but curl has a bunch of known issues with write pausing and decompression.
# Please use libcurl >= 8.17. See https://github.com/curl/curl/issues/19334, https://github.com/curl/curl/issues/16280, https://github.com/curl/curl/issues/16955 if in doubt.
# Patch out this check at your own risk.
curl = dependency('libcurl', 'curl', version : '>= 8.17.0')

deps_private += curl

# seccomp only makes sense on Linux
is_linux = host_machine.system() == 'linux'
seccomp_required = get_option('seccomp-sandboxing')
if not is_linux and seccomp_required.enabled()
  warning('Force-enabling seccomp on non-Linux does not make sense')
endif
seccomp = dependency(
  'libseccomp',
  'seccomp',
  required : seccomp_required,
  version : '>=2.5.5',
)
if is_linux and not seccomp.found()
  warning(
    'Sandbox security is reduced because libseccomp has not been found! Please provide libseccomp if it supports your CPU architecture.',
  )
endif
configdata_priv.set('HAVE_SECCOMP', seccomp.found().to_int())
deps_private += seccomp

if host_machine.system() == 'freebsd'
  # libjail is not in freebsd.libc, but there's no discovery mechanism
  libjail = declare_dependency(link_args : [ '-ljail' ])
  deps_other += libjail
endif

nlohmann_json = dependency('nlohmann_json', version : '>= 3.9')
deps_public += nlohmann_json

sqlite = dependency('sqlite3', 'sqlite', version : '>=3.6.19')
deps_private += sqlite

s3_aws_auth = get_option('s3-aws-auth')
aws_crt_cpp = dependency(
  'aws-crt-cpp',
  required : s3_aws_auth,
  method : 'cmake',
  modules : [ 'AWS::aws-crt-cpp' ],
)

if s3_aws_auth.enabled()
  deps_other += aws_crt_cpp
endif

configdata_pub.set('NIX_WITH_AWS_AUTH', s3_aws_auth.enabled().to_int())

# Experimental PostgreSQL/YugabyteDB metadata backend for the distributed store.
postgres_opt = get_option('postgres')
libpq = dependency('libpq', required : postgres_opt)
if libpq.found()
  deps_private += libpq
endif
configdata_pub.set('NIX_WITH_POSTGRES', libpq.found().to_int())

# Experimental gRPC network transport for the distributed store. The codegen
# and its library are declared later (after all add_project_arguments), since
# meson forbids declaring build targets before project arguments.
grpc_opt = get_option('grpc')
protobuf_dep = dependency('protobuf', required : grpc_opt)
grpcpp_dep = dependency('grpc++', required : grpc_opt)
protoc = find_program('protoc', required : grpc_opt)
grpc_cpp_plugin = find_program('grpc_cpp_plugin', required : grpc_opt)
grpc_enabled = protobuf_dep.found() and grpcpp_dep.found() and protoc.found() and grpc_cpp_plugin.found()
configdata_pub.set('NIX_WITH_GRPC', grpc_enabled.to_int())

subdir('nix-meson-build-support/generate-header')

generated_headers = []
foreach header : [
  'schema.sql',
  'ca-specific-schema.sql',
]
  generated_headers += gen_header.process(header)
endforeach

busybox = find_program(get_option('sandbox-shell'), required : false)

configdata_priv.set(
  'HAVE_EMBEDDED_SANDBOX_SHELL',
  get_option('embedded-sandbox-shell').to_int(),
)

if get_option('embedded-sandbox-shell')
  configdata_priv.set_quoted('SANDBOX_SHELL', '__embedded_sandbox_shell__')
elif busybox.found()
  configdata_priv.set_quoted('SANDBOX_SHELL', busybox.full_path())
endif

if get_option('embedded-sandbox-shell')
  hexdump = find_program('hexdump', native : true)
  embedded_sandbox_shell_gen = custom_target(
    'embedded-sandbox-shell.gen.hh',
    command : [ hexdump, '-v', '-e', '1/1 "0x%x," "\n"' ],
    input : busybox.full_path(),
    output : 'embedded-sandbox-shell.gen.hh',
    capture : true,
    feed : true,
  )
  generated_headers += embedded_sandbox_shell_gen
endif

fs = import('fs')

prefix = get_option('prefix')
# For each of these paths, assume that it is relative to the prefix unless
# it is already an absolute path (which is the default for store-dir, localstatedir, and log-dir).
path_opts = [
  # Meson built-ins.
  'mandir',
  'libdir',
  'includedir',
  'libexecdir',
  'localstatedir',
  # Homecooked Nix directories.
  'store-dir',
  'log-dir',
]
# For your grepping pleasure, this loop sets the following variables that aren't mentioned
# literally above:
# store_dir
# log_dir
# profile_dir
foreach optname : path_opts
  varname = optname.replace('-', '_')
  path = get_option(optname)
  if fs.is_absolute(path)
    set_variable(varname, path)
  else
    set_variable(varname, prefix / path)
  endif
endforeach

# sysconfdir doesn't get anything installed to directly, and is only used to
# tell Nix where to look for nix.conf, so it doesn't get appended to prefix.
if host_machine.system() != 'windows'
  sysconfdir = get_option('sysconfdir')
  if not fs.is_absolute(sysconfdir)
    sysconfdir = '/' / sysconfdir
  endif
endif

# Aside from prefix itself, each of these was made into an absolute path
# by joining it with prefix, unless it was already an absolute path
# (which is the default for store-dir, localstatedir, and log-dir).
configdata_priv.set_quoted('NIX_STORE_DIR', store_dir)
# On Windows, NIX_STATE_DIR, NIX_LOG_DIR, and NIX_CONF_DIR are determined at
# runtime using the Windows known folders API (FOLDERID_ProgramData), so we
# don't define them at compile time.
if host_machine.system() != 'windows'
  configdata_priv.set_quoted('NIX_STATE_DIR', localstatedir / 'nix')
  configdata_priv.set_quoted('NIX_LOG_DIR', log_dir)
  configdata_priv.set_quoted('NIX_CONF_DIR', sysconfdir / 'nix')
endif

lsof = find_program('lsof', required : false)
configdata_priv.set_quoted(
  'LSOF',
  lsof.found() ? lsof.full_path()
  # Just look up on the PATH
: 'lsof',
)

config_priv_h = configure_file(
  configuration : configdata_priv,
  output : 'store-config-private.hh',
)

subdir('nix-meson-build-support/common')

# gRPC codegen + generated-code library (declared here, after all
# add_project_arguments, as meson requires for build targets).
if grpc_enabled
  proto_file = files('distributed/nix-store.proto')
  # Message classes (*.pb.{h,cc}).
  pb = custom_target(
    'nix-store-pb',
    input : proto_file,
    output : ['nix-store.pb.cc', 'nix-store.pb.h'],
    command : [protoc, '--proto_path=@CURRENT_SOURCE_DIR@/distributed', '--cpp_out=@OUTDIR@', '@INPUT@'],
  )
  # Service stubs (*.grpc.pb.{h,cc}).
  grpc_pb = custom_target(
    'nix-store-grpc-pb',
    input : proto_file,
    output : ['nix-store.grpc.pb.cc', 'nix-store.grpc.pb.h'],
    command : [
      protoc,
      '--proto_path=@CURRENT_SOURCE_DIR@/distributed',
      '--grpc_out=@OUTDIR@',
      '--plugin=protoc-gen-grpc=' + grpc_cpp_plugin.full_path(),
      '@INPUT@',
    ],
  )
  # Compile the generated code with warnings-as-errors disabled: it is
  # machine-generated and trips Nix's strict -Werror flags (e.g. unhandled
  # oneof *_NOT_SET cases under -Werror=switch-enum).
  grpc_proto_lib = static_library(
    'nix-store-proto',
    [pb, grpc_pb],
    dependencies : [protobuf_dep, grpcpp_dep],
    # Undo each specific -Werror=… the project sets (a blanket -Wno-error does
    # not override -Werror=<name>), plus quiet the generated noise.
    cpp_args : [
      '-Wno-error=switch-enum',
      '-Wno-error=switch',
      '-Wno-error=sign-compare',
      '-Wno-error=undef',
      '-Wno-error=unused-result',
      '-Wno-error=suggest-override',
      '-Wno-error=non-virtual-dtor',
      '-Wno-error=return-type',
      '-Wno-unused-parameter',
    ],
    pic : true,
  )
  # Generated headers land in this target's build dir, already on the include
  # path via `-Isrc/libstore`. Listing the header outputs as `sources` makes
  # dependents wait for codegen.
  deps_other += declare_dependency(
    link_with : grpc_proto_lib,
    sources : [pb[1], grpc_pb[1]],
    dependencies : [protobuf_dep, grpcpp_dep],
  )
endif

sources = files(
  'binary-cache-store.cc',
  'build-result.cc',
  'build/build-log.cc',
  'build/derivation-builder.cc',
  'build/derivation-building-goal.cc',
  'build/derivation-check.cc',
  'build/derivation-env-desugar.cc',
  'build/derivation-goal.cc',
  'build/derivation-resolution-goal.cc',
  'build/derivation-trampoline-goal.cc',
  'build/drv-output-substitution-goal.cc',
  'build/entry-points.cc',
  'build/goal.cc',
  'build/substitution-goal.cc',
  'build/worker.cc',
  'builtins/buildenv.cc',
  'builtins/fetchurl.cc',
  'builtins/unpack-channel.cc',
  'common-protocol.cc',
  'common-ssh-store-config.cc',
  'content-address.cc',
  'daemon.cc',
  'derivation-options.cc',
  'derivations.cc',
  'derived-path-map.cc',
  'derived-path.cc',
  'downstream-placeholder.cc',
  'dummy-store.cc',
  'export-import.cc',
  'filetransfer.cc',
  'gc.cc',
  'globals.cc',
  'http-binary-cache-store.cc',
  'indirect-root-store.cc',
  'keys.cc',
  'legacy-ssh-store.cc',
  'local-binary-cache-store.cc',
  'local-fs-store.cc',
  'local-gc.cc',
  'local-overlay-store.cc',
  'local-store.cc',
  'log-store.cc',
  'machines.cc',
  'make-content-addressed.cc',
  'misc.cc',
  'names.cc',
  'nar-info-disk-cache.cc',
  'nar-info.cc',
  'optimise-store.cc',
  'outputs-query.cc',
  'outputs-spec.cc',
  'parsed-derivations.cc',
  'path-info.cc',
  'path-references.cc',
  'path-with-outputs.cc',
  'path.cc',
  'pathlocks.cc',
  'posix-fs-canonicalise.cc',
  'profiles.cc',
  'realisation.cc',
  'references.cc',
  'remote-fs-accessor.cc',
  'remote-store.cc',
  'restricted-store.cc',
  's3-binary-cache-store.cc',
  's3-url.cc',
  'serve-protocol-connection.cc',
  'serve-protocol.cc',
  'sqlite.cc',
  'ssh-store.cc',
  'ssh.cc',
  'store-api.cc',
  'store-dir-config.cc',
  'store-reference.cc',
  'store-registration.cc',
  'uds-remote-store.cc',
  'worker-protocol-connection.cc',
  'worker-protocol.cc',
)

# AWS credentials code requires AWS CRT, so only compile when enabled
if s3_aws_auth.enabled()
  sources += files('aws-creds.cc')
endif

# Experimental PostgreSQL/YugabyteDB metadata backend, only compiled with libpq.
if libpq.found()
  sources += files(
    'distributed/postgres-metadata-backend.cc',
    'distributed/distributed-store.cc',
  )
endif

# Experimental gRPC transport: the hand-written server, client, and auth (the
# protoc-generated sources are built separately in `grpc_proto_lib`).
if grpc_enabled
  sources += files(
    'distributed/grpc-store.cc',
    'distributed/grpc-server.cc',
    'distributed/grpc-auth.cc',
  )
endif

subdir('include/nix/store')

if host_machine.system() == 'linux'
  subdir('linux')
endif

if host_machine.system() == 'freebsd'
  subdir('freebsd')
endif

if host_machine.system() == 'darwin'
  subdir('darwin')
endif

if host_machine.system() == 'windows'
  subdir('windows')
else
  subdir('unix')
endif

subdir('nix-meson-build-support/export-all-symbols')
subdir('nix-meson-build-support/windows-version')

this_library = library(
  'nixstore',
  generated_headers,
  sources,
  config_priv_h,
  soversion : nix_soversion,
  dependencies : deps_public + deps_private + deps_other,
  include_directories : include_dirs,
  link_args : linker_export_flags,
  prelink : true, # For C++ static initializers
  install : true,
  cpp_pch : do_pch ? [ 'pch/precompiled-headers.hh' ] : [],
)

install_headers(headers, subdir : 'nix/store', preserve_path : true)

# Standalone launcher for the gRPC store server.
if grpc_enabled
  executable(
    'nix-grpc-store-server',
    'distributed/grpc-server-main.cc',
    link_with : this_library,
    dependencies : deps_public + deps_private + deps_other,
    include_directories : include_dirs,
    install : false,
  )
endif

libraries_private = []

extra_pkg_config_variables = {
  'storedir' : get_option('store-dir'),
  'localstatedir' : get_option('localstatedir'),
}

subdir('nix-meson-build-support/export')
