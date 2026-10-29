#!/usr/bin/env python3
"""Gera as chaves do Airflow no .env, que esta fora do versionamento.

O compose recusa subir sem AIRFLOW_SECRET_KEY e AIRFLOW_FERNET_KEY (interpolacao com
`:?`), de modo que nao existe caminho em que um valor de exemplo vire a chave real por
esquecimento. Valor ja definido nao e sobrescrito: rodar duas vezes nao invalida sessoes
abertas nem torna ilegivel uma Connection ja criptografada com a chave anterior.

Sem dependencia de terceiros: a chave Fernet e exatamente 32 bytes aleatorios em base64
urlsafe, que e o que `cryptography.fernet.Fernet.generate_key()` produz.
"""

import base64
import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(REPO, ".env")
KEYS = ("AIRFLOW_SECRET_KEY", "AIRFLOW_FERNET_KEY")


def main() -> int:
    if not os.path.exists(ENV_PATH):
        print(f"ERRO: {ENV_PATH} nao existe. Copie de .env.example primeiro:")
        print("    cp .env.example .env")
        return 2

    with open(ENV_PATH, encoding="utf-8") as handle:
        text = handle.read()
    changed = False

    for key in KEYS:
        found = re.search(rf"^{key}=(.*)$", text, re.M)
        if found and found.group(1).strip():
            print(f"  {key}: ja definido, mantido")
            continue
        value = base64.urlsafe_b64encode(os.urandom(32)).decode()
        if found:
            text = re.sub(rf"^{key}=.*$", f"{key}={value}", text, flags=re.M)
        else:
            text = text.rstrip("\n") + f"\n{key}={value}\n"
        changed = True
        print(f"  {key}: gerado")

    if changed:
        with open(ENV_PATH, "w", encoding="utf-8") as handle:
            handle.write(text)
        # O arquivo passa a conter chave real; 600 e o modo adequado.
        os.chmod(ENV_PATH, 0o600)
        print(f"  .env gravado com modo 600")
    return 0


if __name__ == "__main__":
    sys.exit(main())
