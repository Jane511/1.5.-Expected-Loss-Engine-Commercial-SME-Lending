# Stress Testing

Stress testing applies scenario multipliers to the three components of expected loss:

- PD multiplier
- LGD multiplier
- CCF multiplier for revolving-facility EAD

The default scenarios are:

| Scenario | PD | LGD | CCF |
|----------|----|-----|-----|
| base | 1.00x | 1.00x | 1.00x |
| mild | 1.25x | 1.10x | 1.10x |
| severe | 2.00x | 1.30x | 1.20x |

The stress output reports:

- total stressed EAD
- total stressed expected loss
- average stressed PD
- average stressed LGD
- change versus base
- percentage change versus base

This framing is enough to support interview discussion around downturn overlays, migration risk, and sensitivity of revolving products to adverse conditions.

The stress output is also shaped so `stress-testing-commercial` can consume expected-loss measures without relying on local workspace-specific folder conventions.
