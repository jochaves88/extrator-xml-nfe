import os
import shutil
import zipfile
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse
from typing import List

app = FastAPI()

# --- LÓGICA DE EXTRAÇÃO (A MESMA DO COLAB) ---
ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

def pegar_valor(no, caminho, tipo=str):
    try:
        r = no.find(caminho, ns)
        if r is not None and r.text:
            return tipo(r.text)
        return tipo(0) if tipo in [float, int] else ""
    except:
        return tipo(0) if tipo in [float, int] else ""

def buscar_imposto_item(detalhe, tag_imposto, tag_filha):
    imposto_node = detalhe.find(f'nfe:imposto/nfe:{tag_imposto}', ns)
    if imposto_node is None: return 0.0
    for filho in imposto_node:
        valor = filho.find(f'nfe:{tag_filha}', ns)
        if valor is not None: return float(valor.text)
    return 0.0

def processar_xmls(pasta_xml):
    arquivos = glob.glob(f"{pasta_xml}/**/*.xml", recursive=True)
    dados = []
    
    for arq in arquivos:
        try:
            tree = ET.parse(arq)
            root = tree.getroot()
            if root.tag.endswith('nfeProc'):
                inf_nfe = root.find('nfe:NFe/nfe:infNFe', ns)
            else:
                inf_nfe = root.find('nfe:infNFe', ns)
            if inf_nfe is None: continue

            # Dados Gerais
            ide = inf_nfe.find('nfe:ide', ns)
            emit = inf_nfe.find('nfe:emit', ns)
            dest = inf_nfe.find('nfe:dest', ns)
            total = inf_nfe.find('nfe:total/nfe:ICMSTot', ns)
            
            chave = pegar_valor(root.find('nfe:protNFe/nfe:infProt', ns), 'nfe:chNFe')
            if not chave: chave = inf_nfe.attrib.get('Id', '')[3:]
            data_raw = pegar_valor(ide, 'nfe:dhEmi') or pegar_valor(ide, 'nfe:dEmi')
            data_nfe = data_raw[:10]
            
            dets = inf_nfe.findall('nfe:det', ns)
            for det in dets:
                prod = det.find('nfe:prod', ns)
                item = {
                    'Mês': data_nfe[5:7],
                    'Ano': data_nfe[:4],
                    'Chave Acesso NFe': "'" + chave,
                    'Razão Social Emitente': pegar_valor(emit, 'nfe:xNome'),
                    'Cnpj Emitente': pegar_valor(emit, 'nfe:CNPJ'),
                    'Nr NFe': pegar_valor(ide, 'nfe:nNF', int),
                    'Data NFe': data_nfe,
                    'Vr Total': pegar_valor(prod, 'nfe:vProd', float),
                    'Descrição Produto NFe': pegar_valor(prod, 'nfe:xProd'),
                    'NCM na NFe': pegar_valor(prod, 'nfe:NCM'),
                    'CFOP NFe': pegar_valor(prod, 'nfe:CFOP', int),
                    'Qtde': pegar_valor(prod, 'nfe:qCom', float),
                    'Vr Unit': pegar_valor(prod, 'nfe:vUnCom', float),
                    # Adicionei apenas as principais para o exemplo ficar curto, 
                    # mas o código aceita todas as 33 colunas se você colar a lista completa aqui
                }
                dados.append(item)
        except:
            pass
            
    df = pd.DataFrame(dados)
    return df

# --- ROTAS DO SITE ---

@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <html>
        <head>
            <title>Extrator XML NFe</title>
            <style>
                body { font-family: sans-serif; text-align: center; padding: 50px; background: #f4f4f9; }
                .box { background: white; padding: 40px; border-radius: 10px; box-shadow: 0 0 10px rgba(0,0,0,0.1); display: inline-block; }
                h1 { color: #333; }
                input { margin: 20px 0; }
                button { background: #28a745; color: white; border: none; padding: 10px 20px; font-size: 16px; cursor: pointer; border-radius: 5px; }
                button:hover { background: #218838; }
            </style>
        </head>
        <body>
            <div class="box">
                <h1>Extrator de XML NFe 🚀</h1>
                <p>Envie seu arquivo ZIP com as notas fiscais.</p>
                <form action="/upload" method="post" enctype="multipart/form-data">
                    <input type="file" name="file" accept=".zip" required>
                    <br>
                    <button type="submit">Processar e Baixar Excel</button>
                </form>
            </div>
        </body>
    </html>
    """

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    temp_dir = "temp_files"
    if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
    
    zip_path = os.path.join(temp_dir, "arquivo.zip")
    with open(zip_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
        
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
        
    df = processar_xmls(temp_dir)
    
    output_file = "Relatorio_Processado.xlsx"
    df.to_excel(output_file, index=False)
    
    return FileResponse(output_file, filename="Relatorio_Notas.xlsx", media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')