## Usage Instructions

### Untar tarball

```sh
tar -xvf sorting.tar
cd sorting/src
```

### Run program
To run the external sorting program given an input file names `input.dat` where each record is 100 bytes, key size is 8 bytes, sorted in ascending order (indicated with 1), with 32MB memory, the command to run is:
```sh
python3 extsort.py input.dat output.dat 100 8 32 1
```
