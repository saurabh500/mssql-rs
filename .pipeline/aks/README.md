
```
az login
az aks get-credentials --resource-group rust-lib-rg --name CargoCatalog
```

# Replace <your-sa-password> with your actual SQL Server SA password
kubectl create secret generic sqlserver-secret --from-literal=sa-password=your-actual-password


kubectl apply -f sql.yaml
kubectl apply -f service.yaml

