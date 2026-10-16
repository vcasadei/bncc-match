{
  description = "Ambiente de desenvolvimento do bncc-match (Python 3.12 + uv + ffmpeg)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            python312
            uv
            ffmpeg
          ];
          shellHook = ''
            echo "bncc-match: ambiente ativo (Python $(python --version | awk '{print $2}'), uv $(uv --version | awk '{print $2}'))"
          '';
        };
      });
}
