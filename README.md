# plato-tile-cascade

Dependency cascade engine — propagate tile updates downstream.

```rust
use plato_tile_cascade::TileCascade;

let mut c = TileCascade::new();
c.add_dependency("base", "derived");
let result = c.update_tile("base");
```

Zero external dependencies.
