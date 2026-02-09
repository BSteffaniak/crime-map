{
  description = "Crime Map - US crime data visualization";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [
            "rust-src"
            "rust-analyzer"
            "clippy"
            "rustfmt"
          ];
        };

        baseBuildTools = with pkgs; [
          pkg-config
          gnumake
          gcc
          libiconv
          autoconf
          automake
          libtool
          cmake
          openssl
        ];

        geoTools = with pkgs; [
          gdal # ogr2ogr for FlatGeobuf generation
          tippecanoe # PMTiles generation
        ];

      in
      {
        devShells = {
          default = pkgs.mkShell {
            buildInputs = [
              rustToolchain
              pkgs.fish
            ]
            ++ baseBuildTools
            ++ geoTools
            ++ (with pkgs; [
              postgresql # psql client
              docker-compose
              nodejs
              bun
            ])
            ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
              pkgs.clang
            ];

            shellHook = ''
              echo "Crime Map Development Environment"
              echo "Rust: $(rustc --version)"
              echo ""
              echo "Commands:"
              echo "  docker compose up -d     Start PostGIS (port 5440)"
              echo "  cargo build              Build all Rust packages"
              echo "  cargo run -p crime_map_server  Start API server"
              echo "  cd app && bun dev       Start frontend dev server"

              ${pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
                export CC="${pkgs.clang}/bin/clang"
                export CXX="${pkgs.clang}/bin/clang++"
              ''}

              # Only exec fish if we're in an interactive shell (not running a command)
              if [ -z "$IN_NIX_SHELL_FISH" ] && [ -z "$BASH_EXECUTION_STRING" ]; then
                case "$-" in
                  *i*) export IN_NIX_SHELL_FISH=1; exec fish ;;
                esac
              fi
            '';
          };

          ci = pkgs.mkShell {
            buildInputs = [
              rustToolchain
            ]
            ++ baseBuildTools
            ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
              pkgs.clang
            ];
          };
        };
      }
    );
}
