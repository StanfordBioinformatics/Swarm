select
  a.reference_name,
  a.start_position,
  a.end_position,
  a.reference_bases,
  a.alternate_bases,
  @samplesa,
  @samplesb
from
  swarm.%s b
full outer join
  swarm.%s a
  on a.reference_name = b.reference_name
  and a.start_position = b.start_position
--  and a.end_position = b.end_position
  and a.reference_bases = b.reference_bases
  and a.alternate_bases = b.alternate_bases
-- group by a.reference_name, a.pos, a.ref, a.alt