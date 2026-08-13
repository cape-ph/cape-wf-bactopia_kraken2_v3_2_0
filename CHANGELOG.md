# Changelog

## [0.2.0](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/compare/v0.1.4...v0.2.0) (2026-08-13)


### Features

* add report generation ([e4e3ef8](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/e4e3ef836862d67789ed452f785616a572e171c7))
* initial pass at report generation in the bactopia workflow ([c1b3a03](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/c1b3a034c5ee58999451ca22e4b181bbf7a1beb5))
* **report:** store reports in the seqauto artifacts bucket ([d41929f](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/d41929f27ebca3086f7fd79137b904b5622301b6))


### Bug Fixes

* **report:** also run and wait for the seqauto input-clean crawler ([5a3a38e](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/5a3a38ec738700cf324d1f9e57b81dad485a5b3a))


### Documentation

* fix typo ([86eb925](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/86eb925686b67a69365373eefbc152d872ad701a))
* sync README to dag_run.conf config format and add project wiki ([5e7cf21](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/5e7cf219ec5c9aebd7c7f0cf3f44a8561f4e27a7))

## [0.1.4](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/compare/v0.1.3...v0.1.4) (2026-08-11)


### Bug Fixes

* 9 modify the configuration format for the dag ([c02762d](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/c02762d86a65f9dcc4f6b1df0180e0bbf0c67ae0))
* wired up for new config format. refactored to (hopefully) make extraction of common DAG code into a library down the road ([3279dcc](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/3279dccc0bf8ad02078240aa678c2e1e63cbaf68))

## [0.1.3](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/compare/v0.1.2...v0.1.3) (2026-06-04)


### Bug Fixes

* incorrect dag id in metadata causing system issued down the line. ([7d3df10](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/7d3df10df2df53ac7237dd35df5ca42656495ce3))
* incorrect dag id in metadata causing system issued down the line. ([0f68b57](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/0f68b57406209eb3c5ea4679b7cabfcac3687231))

## [0.1.2](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/compare/v0.1.1...v0.1.2) (2026-06-03)


### Bug Fixes

* added in missing meta.json content ([c05ec54](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/c05ec54b24d43dfa814b228370ea01dc282531f2))
* added in missing meta.json content ([e284b80](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/e284b8050fd4a66ebbe92be98086f1c5e59c737c))

## [0.1.1](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/compare/v0.1.0...v0.1.1) (2026-06-03)


### Bug Fixes

* correct JQ syntax error in release artifact detection ([b617d22](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/b617d22cc6234d281bf2288a76a80f093382d463))
* correct JQ syntax error in release artifact detection ([ca44574](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/ca445743dc01b90d6be89c795a4729f449e6a2c9))
* Merge pull request [#3](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/issues/3) from cape-ph/fix-release-asset-naming-again ([b617d22](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/b617d22cc6234d281bf2288a76a80f093382d463))

## 0.1.0 (2026-06-03)


### Bug Fixes

* change pyproject.toml we're using poetry only for depends managent, not for packaging ([48b5f79](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/48b5f790a806cdad5bf526c4cfbf3bcc49a95d88))
* constrain Python version for Airflow 3.1 compatibility ([11fbe06](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/11fbe065fdb6ae61e5a8d1967cfac3699252c4b6))
* fix release asset naming ([70eec84](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/70eec84d0a33fab094cfdb6fff6ecf56defe806e))
* improve release artifact attachment workflow ([b5f9350](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/b5f9350d373d68b652e60f31bc80e67caaa78c6b))
* Merge pull request [#1](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/issues/1) from cape-ph/fix-release-asset-naming to main ([70eec84](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/70eec84d0a33fab094cfdb6fff6ecf56defe806e))
* updated airflow and python versions to match what is deployed and what the dag is written against ([466171f](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/466171fd304ba5d1ffe8826c272d78a24f4d9d54))


### Documentation

* add operations guide and reorganize documentation ([affb7a7](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/commit/affb7a760ab42c092c240dfd1f5c1e15893e5f27))

## Changelog

All notable changes to this project will be documented in this file. See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.
