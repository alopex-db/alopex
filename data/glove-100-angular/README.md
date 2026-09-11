# GloVe 100-angular benchmark input

This directory is the pinned data store for issue #298. The HDF5 file is split
into parts because GitHub rejects a single Git blob above 100 MiB.

- Source: https://ann-benchmarks.com/glove-100-angular.hdf5
- Size: 485,413,888 bytes
- SHA-256: `544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4`
- Parts: `glove-100-angular.hdf5.part-*` (concatenate in lexical order)

Reconstruct the file from the repository root:

```sh
cat data/glove-100-angular/glove-100-angular.hdf5.part-* \
  > data/glove-100-angular.hdf5
sha256sum data/glove-100-angular.hdf5
```

The benchmark runner must use the reconstructed file and verify the checksum
before measuring. The runner must not download the dataset at runtime.
