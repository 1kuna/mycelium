git clone https://github.com/1kuna/mycelium.git
cd mycelium
go build -o myce ./cmd/myce
./myce dev -- sh examples/quickstart.sh
cat quickstart-result/result.txt
