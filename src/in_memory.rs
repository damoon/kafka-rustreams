use super::Topology;

pub struct Driver<'a> {
    topo: Topology<'a>,
}

impl<'a> Driver<'a> {
    pub fn new(topo: Topology) -> Driver {
        Driver {
            topo
        }
    }

    pub fn write_to(&mut self, topic: &str, msg: Option<&[u8]>) {}
}
