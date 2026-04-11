# Pricing Logic

Expected loss is useful, but on its own it is not a pricing answer. The pricing module converts the loss rate into a simple hurdle framework:

```text
Required Margin = EL Rate + Funding Cost + Operating Cost + Target Return
```

Default assumptions in this repo:

- Funding cost: 1.8%
- Operating cost: 0.7%
- Target return: 2.5%

The output table shows, by product:

- weighted EL rate
- weighted average booked interest rate
- required margin
- pricing gap
- share of facilities meeting or exceeding hurdle

This is deliberately simple. It is enough to show how expected loss can feed pricing support without pretending to be a full RAROC or capital-allocation engine.

The pricing views are designed to feed `RAROC-pricing-and-return-hurdle` as a downstream commercial pricing and return-hurdle layer.
