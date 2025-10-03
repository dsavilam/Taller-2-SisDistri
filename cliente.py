import argparse, sys, os
import grpc
from typing import List

# Stubs generados
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'generated'))
import distcalc_pb2, distcalc_pb2_grpc

TAMANO_REQUERIDO = 5

def leer_arreglo_usuario(nombre_arreglo: str, tam_esperado: int = TAMANO_REQUERIDO) -> List[int]:
    print(f"Ingrese {tam_esperado} enteros para el {nombre_arreglo}, separados por espacios:")
    linea = input().strip()
    if not linea:
        print(f"No ingresaste valores para {nombre_arreglo}.")
        sys.exit(1)

    partes = linea.split()
    if len(partes) > tam_esperado:
        print("Te pedi ingresar 5 numeros tontin, ingresaste mas y el arreglo se desbordo")
        sys.exit(1)
    if len(partes) < tam_esperado:
        print(f"Ingresaste menos de {tam_esperado} números para {nombre_arreglo}.")
        sys.exit(1)

    try:
        return [int(x) for x in partes]
    except ValueError:
        print("Entrada inválida: asegúrate de ingresar solo enteros separados por espacios.")
        sys.exit(1)

def main():
    ap = argparse.ArgumentParser(description="Cliente gRPC del Coordinador")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    args = ap.parse_args()

    a = leer_arreglo_usuario("primer arreglo (A)")
    b = leer_arreglo_usuario("segundo arreglo (B)")
    print(f"[cliente] A={a}")
    print(f"[cliente] B={b}")

    with grpc.insecure_channel(f"{args.host}:{args.port}") as channel:
        stub = distcalc_pb2_grpc.CoordinatorServiceStub(channel)
        resp = stub.SumArrays(distcalc_pb2.SumArraysRequest(a=a, b=b), timeout=20.0)

    if not resp.ok:
        print(f"[cliente] Error: {resp.error}")
        return

    esperado_local = [x + y for x, y in zip(a, b)]
    ok = (list(resp.result) == esperado_local)
    print(f"[cliente] OK={ok} | tiempo={resp.elapsed:.4f}s")
    print(f"[cliente] Resultado={list(resp.result)}")

    # Esto no lo imprimimos en la suste porque como dijo el profe
    # tiene que ser transparente al usuario 
    
    #print("[cliente] Operadores:")
    #for op in resp.operadores:
        #estado = "VIVO" if op.vivo else "DOWN"
        #print(f"  - {op.nombre} @ {op.host}:{op.puerto} -> {estado}")

if __name__ == "__main__":
    main()
