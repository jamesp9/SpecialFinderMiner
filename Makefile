EXPORT_CONF=export SPECIAL_FINDER_MINER=./dev.yml

miner:
	${EXPORT_CONF};./SpecialFinderMiner/miner.py


transmitter:
	${EXPORT_CONF};./SpecialFinderMiner/transmitter.py


dumper:
	${EXPORT_CONF};./SpecialFinderMiner/dumper.py
