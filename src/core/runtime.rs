//
// Config for building HioRuntime
//

pub struct HioRuntimeConfig {}

impl HioRuntimeConfig {
    pub fn new() -> Self {
        Self {}
    }
}

//
// Context for managing the hio runtime, including thread pool, blocking queue, and other resources.
//

pub struct HioRuntime {}

impl HioRuntime {
    pub fn new(cfg_ref: &HioRuntimeConfig) -> Self {
        Self {}
    }
}
