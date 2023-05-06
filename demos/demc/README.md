# Differential Evolution Monte Carlo (DEMC) demo

## Summary

This example shows how to implement a simple variant of the DEMC algorithm by Cajo J. F. Ter Braak and Jasper A. Vrugt.

Key references:

- ter Braak, C.J.F. A Markov Chain Monte Carlo version of the genetic algorithm Differential Evolution: easy Bayesian computing for real parameter spaces. Stat Comput 16, 239–249 (2006). https://doi.org/10.1007/s11222-006-8769-1
- ter Braak, C.J.F., Vrugt, J.A. Differential Evolution Markov Chain with snooker updater and fewer chains. Stat Comput 18, 435–446 (2008). https://doi.org/10.1007/s11222-008-9104-9


## Implementation details

- 5 parallel MC walkers are used.
- A thinned history of each walker is maintained containing at most 100 samples.
  When a new sample is added and the history length exceeds 100, the oldest is removed.
  For the sake of ergodicity in the context of adaptive MC,
  the stride between two items in the history gradually increases,
  such that they eventually become independent of each other and of the current point.
- As soon as the history reaches a length of 2, each walker receives the combined history of the other walkers, from which it can draw differences between historical points to construct MC moves.
  In all iterations (even in the absence of any history), a small Cauchy distortion is included in the MC move.

The algorithm is applied to a well-known pathological regression problem, i.e. fitting polynomial
to (non-polynomial) data with some noise.
The data is a circular arc on the domain $[-1, 1]$ with normal noise $\mathcal{N}(\mu=0, \sigma=0.1)$.
The polynomial model is of degree 20.
This is just a simple example that can be easily compared to the linear algebra result.
Of course, the DEMC algorithm also works for nonlinear ill-conditioned problems.


## Scripts

- `linreg.py` is a standard linear regression implementation, for reference.
- `naivemc.py` implements a naive MC chain with a fixed proposal distribution.
  It plots the parameters every 50 MC steps and compares the sampling covariance eigenvalues with the expected ones.
- `demc.py` implements the DEMC algorithm, using Parman for parallelization.
  The samples from all walkers is combined for the final analysis.

The two MC implementations are configured to compute the posterior about $10^5$ times.


## Remarks

- The burn-in is configured ad hoc in both scripts.
- The results show that, as expected, naive MC cannot explore the full parameter distribution.
  DEMC overcomes this problem by drawing step sizes from the distribution of differences between historical points.
- No regularization is applied to the posterior, so converged sampling results in an overfitted model.
  The naive MC implementation effectively regularizes the fit due to its inefficient sampling, resulting in a smoother polynomial fit.
  This may seem convenient at first, but this type of regularization is difficult to control.
- To achieve parallel efficiency, a more expensive loss function would be required, making the DEMC and communication overhead negligible.
- This example of DEMC is small and hackable, suitable for learning the algorithm or for customization beyond standard implementations.
  More polished implementations such as [emcee](https://emcee.readthedocs.io) may offer a better experience for those interested in a canned solution.
