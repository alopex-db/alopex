# Pinned cargo-deny action

`action.yml` and `entrypoint.sh` are vendored without behavioral changes from
[EmbarkStudios/cargo-deny-action at b66acf5](https://github.com/EmbarkStudios/cargo-deny-action/tree/b66acf5e9fe20f8aba065be86778a8a4c846f902),
under the included MIT license. The container base digest, cargo-deny 0.20.2,
input arguments and entrypoint remain unchanged.

The Docker build fetches the same release archive through its public GitHub
asset API and checks its byte length and SHA256 before extraction. This avoids
passing a download error response to tar. No credentials are supplied.

`download.sh` is also used for the pinned TLC JAR. It limits transfer size and
retry time, verifies bytes in a temporary file, and only then replaces the
destination. Failed downloads remove their temporary file.
