# SweetFuture Demo

For now, this demo is the only documentation.
The demo does not do much: it has the structure of an active learning loop,
in which each job does a bit of file IO and a 1 second sleep call.

Good luck!


## Timings

Number of jobs in demo: 713 (11m53 without IO or other overheads)

Wall times:

- serial: 12m36.267s
- concurrent (thread): 1m48.040s
- concurrent (proc): 1m46.563s
- parsl-local: 1m48.723s
- dask: 2m12.624s
