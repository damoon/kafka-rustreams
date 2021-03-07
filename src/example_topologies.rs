use super::Topology;

pub fn copy(source: &'static str, target: &'static str) -> Topology {
    let mut topology = Topology::new();

    let input = topology.read_from(source);
    input.write_to(target);

    topology
}

pub fn noop() -> Topology {
    Topology::new()
}
