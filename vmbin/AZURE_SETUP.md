# Banco en Azure — VM tipo scorer (Intel Xeon, 8 vCPU)

## 1. Instalar Azure CLI (una vez)

```powershell
winget install -e --id Microsoft.AzureCLI
```

## 2. Login (abre el navegador con tu cuenta de estudiante)

```bash
az login
```

## 3. Crear la VM (~$0.38/h — los $100 dan para ~260h)

```bash
az group create -n helex-bench -l eastus
az vm create -g helex-bench -n rig \
  --size Standard_D8s_v5 \
  --image Ubuntu2204 \
  --admin-username helex \
  --generate-ssh-keys \
  --public-ip-sku Standard
```

`Standard_D8s_v5` = Intel Ice Lake (8 vCPU, 32 GB) — la familia más cercana al n2 del scorer.

**Si el SKU no está permitido en tu suscripción de estudiante**, prueba en orden:
`Standard_D8s_v4` (Cascade Lake) → `Standard_D8s_v3` (Skylake-SP, tiene AVX-512) → `Standard_D4s_v5` (4 vCPU).

Si da error de cuota: `az vm list-usage -l eastus --query "[?name.value=='standardDSv5Family']"` y cambia de región (`westus2`, `westeurope`).

## 4. Subir los binarios

```bash
IP=$(az vm show -g helex-bench -n rig -d --query publicIps -o tsv)
scp -r vmbin helex@$IP:~/
```

## 5. Correr el banco (intercalado, caché fría)

```bash
ssh helex@$IP 'chmod +x ~/vmbin/* && cd ~/vmbin && ./vm_bench.sh 6 ./ss-alex ./ss-cand'
```

Genera los datos la primera vez (~30s), luego corre 6 rondas intercaladas.

## 6. Apagar para no gastar crédito

```bash
az vm deallocate -g helex-bench -n rig   # deallocated = no cobra CPU (disco sí, centavos)
az vm start -g helex-bench -n rig        # encender de nuevo
az group delete -n helex-bench           # borrar todo al terminar
```
