# "Plastic" deformations of atomistic structures

This is work in progress. More testing is needed.

Like all things made of plastic, the results will at best look real.
The samples should be better than simple Cartesian random displacements.

Usage:

```bash
./plastic.py alumina.json alumina.traj alumina_final.xyz
./plastic.py ibuprofen.xyz ibuprofen.traj ibuprofen_final.xyz
```

The default settings should be fine, but can be tweaked, see `./plastic.py --help`.

Visualization:

```bash
ase gui alumina.traj
ase gui ibuprofen.traj
```
