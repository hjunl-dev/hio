//
// Transport Handler
//

pub trait TransportHandler: Send + Sync + 'static {
    fn on_connect(&self);
    fn on_data(&self, data: &[u8]);
    fn on_close(&self);
}
