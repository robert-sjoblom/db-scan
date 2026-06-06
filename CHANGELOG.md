# Changelog

## [0.4.0](https://github.com/robert-sjoblom/db-scan/compare/v0.3.0...v0.4.0) (2026-06-06)


### Features

* **analyze:** flag primaries that cannot satisfy sync quorum ([c4cabbb](https://github.com/robert-sjoblom/db-scan/commit/c4cabbbf9daced2605156995fd2311b8de350f08))
* **analyze:** refuse to resolve split-brain when sysids disagree ([#59](https://github.com/robert-sjoblom/db-scan/issues/59)) ([5050ba2](https://github.com/robert-sjoblom/db-scan/commit/5050ba2a8bfbfa4363771807a3335af5084ee84c))
* **analyze:** refuse to resolve when synchronous commit is weakened ([22fc98d](https://github.com/robert-sjoblom/db-scan/commit/22fc98d7efacaabe4c9b38e0a48e2addb2979499))
* **analyze:** require live, bidirectional streaming in split-brain gate ([#60](https://github.com/robert-sjoblom/db-scan/issues/60)) ([e71ec49](https://github.com/robert-sjoblom/db-scan/commit/e71ec4903938909960d90a3c6f42a02540a3e7ab))
* Remote capture: persist per-run pipeline state to internal postgres ([#57](https://github.com/robert-sjoblom/db-scan/issues/57)) ([b6d3c4b](https://github.com/robert-sjoblom/db-scan/commit/b6d3c4b9db433b793cc2db618da7c630f1c46c3c))


### Bug Fixes

* **analyze:** stop quorum-unsatisfied findings from refusing own resolution ([87e51e5](https://github.com/robert-sjoblom/db-scan/commit/87e51e58496e4ae3280fc7efcf7326aacbb7595f))

## [0.3.0](https://github.com/robert-sjoblom/db-scan/compare/v0.2.3...v0.3.0) (2026-05-25)


### Features

* add archive lagging as a reason ([37ace1f](https://github.com/robert-sjoblom/db-scan/commit/37ace1f033a7cc649b34b9538053928b6472a077))
* add Confidence and findings to SplitBrainInfo ([#55](https://github.com/robert-sjoblom/db-scan/issues/55)) ([64979a3](https://github.com/robert-sjoblom/db-scan/commit/64979a34b2c83e3458f816bfc3ca2c510394fcee))
* collect timeline-history file and add fork-LSN parser ([7ef6737](https://github.com/robert-sjoblom/db-scan/commit/7ef6737511ea0a1adad12ff47d74d42668841ac5))
* ensure each health check captures a current_time ([a37cae2](https://github.com/robert-sjoblom/db-scan/commit/a37cae209c4917035e28710540864f231d916409))


### Bug Fixes

* add current_time to primary health check + use , not : in json object syntax ([#56](https://github.com/robert-sjoblom/db-scan/issues/56)) ([c091630](https://github.com/robert-sjoblom/db-scan/commit/c09163065d10f6e8797380375b53f2a1bcd33682))

## [0.2.3](https://github.com/robert-sjoblom/db-scan/compare/v0.2.2...v0.2.3) (2026-05-18)


### Bug Fixes

* check full ring buffer for disk checks ([ede6aa8](https://github.com/robert-sjoblom/db-scan/commit/ede6aa85719974cd335ad34feef577246a3195a9))
* expose last failed at for WAL archive failures ([5b39f53](https://github.com/robert-sjoblom/db-scan/commit/5b39f532a403dec44e8d0de6d201e65a53f0c23a))

## [0.2.2](https://github.com/robert-sjoblom/db-scan/compare/v0.2.1...v0.2.2) (2026-05-17)


### Bug Fixes

* use correct bin path in archive for self-update ([e269d92](https://github.com/robert-sjoblom/db-scan/commit/e269d92d697f5649086aa7a28de49f82647043b2))

## [0.2.1](https://github.com/robert-sjoblom/db-scan/compare/v0.2.0...v0.2.1) (2026-05-17)


### Bug Fixes

* use rustls for self-update ([2ec9770](https://github.com/robert-sjoblom/db-scan/commit/2ec977098a0a2aa7f238a9dcadfd03f127026c63))

## [0.2.0](https://github.com/robert-sjoblom/db-scan/compare/v0.1.1...v0.2.0) (2026-05-17)


### Features

* implement self-update ([f7930d2](https://github.com/robert-sjoblom/db-scan/commit/f7930d2ee38875657dc3d5f9d966b4befddf113a))

## [0.1.1](https://github.com/robert-sjoblom/db-scan/compare/0.1.0...v0.1.1) (2026-05-17)


### Bug Fixes

* update error-stack ([74d9740](https://github.com/robert-sjoblom/db-scan/commit/74d9740f6c375063caf852969d4eaa4e41c03dd6))
