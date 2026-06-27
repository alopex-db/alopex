{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    roc.url = "github:roc-lang/roc";
  };

  outputs = { nixpkgs, roc, ... }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
      rocPkgs = roc.packages.${system};
    in {
      devShells.${system}.default = pkgs.mkShell {
        buildInputs = [
          rocPkgs.cli
        ];
      };
    };
}
