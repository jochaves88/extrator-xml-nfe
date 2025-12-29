import os
import shutil
import zipfile
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse

app = FastAPI()

# --- CONFIGURAÇÃO ---
ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

def pegar_valor(no, caminho, tipo=str):
    """Busca valor de forma segura. Retorna 0.0 para numeros se nao encontrar."""
    if no is None: return tipo(0) if tipo in [float, int] else ""
    try:
        r = no.find(caminho, ns)
        if r is not None and r.text:
            return tipo(r.text.replace(',', '.'))
        return tipo(0) if tipo in [float, int] else ""
    except:
        return tipo(0) if tipo in [float, int] else ""

def processar_xmls(pasta_xml):
    arquivos = glob.glob(f"{pasta_xml}/**/*.xml", recursive=True)
    arquivos += glob.glob(f"{pasta_xml}/**/*.XML", recursive=True)
    
    dados = []
    print(f"Iniciando processamento de {len(arquivos)} arquivos...")
    
    for arq in arquivos:
        try:
            tree = ET.parse(arq)
            root = tree.getroot()
            
            # Ajuste de namespace para NFeProc ou NFe pura
            if root.tag.endswith('nfeProc'):
                inf_nfe = root.find('nfe:NFe/nfe:infNFe', ns)
            else:
                inf_nfe = root.find('nfe:infNFe', ns)
            
            if inf_nfe is None: continue

            # --- BLOCOS PRINCIPAIS ---
            ide = inf_nfe.find('nfe:ide', ns)
            emit = inf_nfe.find('nfe:emit', ns)
            dest = inf_nfe.find('nfe:dest', ns)
            total_icms = inf_nfe.find('nfe:total/nfe:ICMSTot', ns) # Totais da Nota
            
            # --- DADOS GERAIS DA NOTA ---
            chave = pegar_valor(root.find('nfe:protNFe/nfe:infProt', ns), 'nfe:chNFe')
            if not chave: chave = inf_nfe.attrib.get('Id', '')[3:]
            
            data_raw = pegar_valor(ide, 'nfe:dhEmi')
            if not data_raw: data_raw = pegar_valor(ide, 'nfe:dEmi')
            data_nfe = data_raw[:10] # Formato AAAA-MM-DD
            
            # Mes e Ano
            ano = data_nfe[:4]
            mes = data_nfe[5:7]

            # --- LOOP DE PRODUTOS ---
            dets = inf_nfe.findall('nfe:det', ns)
            for det in dets:
                prod = det.find('nfe:prod', ns)
                imposto = det.find('nfe:imposto', ns)
                
                # Variaveis de Imposto do ITEM (zeradas por padrao)
                cst_csosn = ""
                bc_icms_item = 0.0
                aliq_icms_item = 0.0
                vr_icms_item = 0.0
                aliq_ipi_item = 0.0
                vr_ipi_item = 0.0
                
                # Busca Inteligente de CST/CSOSN e ICMS
                if imposto is not None:
                    icms_node = imposto.find('nfe:ICMS', ns)
                    if icms_node:
                        for child in icms_node:
                            # Tenta pegar CST ou CSOSN
                            cst_csosn = pegar_valor(child, 'nfe:CST')
                            if not cst_csosn: cst_csosn = pegar_valor(child, 'nfe:CSOSN')
                            
                            # Valores do ICMS Item
                            bc_icms_item = pegar_valor(child, 'nfe:vBC', float)
                            aliq_icms_item = pegar_valor(child, 'nfe:pICMS', float)
                            vr_icms_item = pegar_valor(child, 'nfe:vICMS', float)

                    # Busca IPI
                    ipi_node = imposto.find('nfe:IPI', ns)
                    if ipi_node:
                        ipitrib = ipi_node.find('nfe:IPITrib', ns)
                        if ipitrib:
                            aliq_ipi_item = pegar_valor(ipitrib, 'nfe:pIPI', float)
                            vr_ipi_item = pegar_valor(ipitrib, 'nfe:vIPI', float)

                # --- MONTAGEM DA LINHA ---
                item = {
                    'Mês': mes,
                    'Ano': ano,
                    'Chave Acesso NFe': "'" + chave,
                    'Inscrição Destinatário': pegar_valor(dest, 'nfe:IE'),
                    'Inscrição Emitente': pegar_valor(emit, 'nfe:IE'),
                    'Razão Social Emitente': pegar_valor(emit, 'nfe:xNome'),
                    'Cnpj Emitente': pegar_valor(emit, 'nfe:CNPJ'),
                    'UF Emitente': pegar_valor(emit, 'nfe:enderEmit/nfe:UF'),
                    'Nr NFe': pegar_valor(ide, 'nfe:nNF'),
                    'Série': pegar_valor(ide, 'nfe:serie'),
                    'Data NFe': data_nfe,
                    
                    # Totais da Nota (Cabecalho)
                    'BC ICMS Total': pegar_valor(total_icms, 'nfe:vBC', float),
                    'ICMS Total': pegar_valor(total_icms, 'nfe:vICMS', float),
                    'BC ST Total': pegar_valor(total_icms, 'nfe:vBCST', float),
                    'ICMS ST Total': pegar_valor(total_icms, 'nfe:vST', float),
                    'Desc Total': pegar_valor(total_icms, 'nfe:vDesc', float),
                    'IPI Total': pegar_valor(total_icms, 'nfe:vIPI', float),
                    'Total Produtos': pegar_valor(total_icms, 'nfe:vProd', float),
                    'Total NFe': pegar_valor(total_icms, 'nfe:vNF', float),
                    
                    # Dados do Produto (Item)
                    'Descrição Produto NFe': pegar_valor(prod, 'nfe:xProd'),
                    'NCM na NFe': pegar_valor(prod, 'nfe:NCM'),
                    'CST': cst_csosn,
                    'CFOP NFe': pegar_valor(prod, 'nfe:CFOP'),
                    'Qtde': pegar_valor(prod, 'nfe:qCom', float),
                    'Unid': pegar_valor(prod, 'nfe:uCom'),
                    'Vr Unit': pegar_valor(prod, 'nfe:vUnCom', float),
                    'Vr Total': pegar_valor(prod, 'nfe:vProd', float), # Valor total do produto
                    'Desconto Item': pegar_valor(prod, 'nfe:vDesc', float),
                    
                    # Impostos do Item
                    'Base de Cálculo ICMS': bc_icms_item,
                    'Aliq ICMS': aliq_icms_item,
                    'Vr ICMS': vr_icms_item,
                    'Aliq IPI': aliq_ipi_item,
                    'Vr IPI': vr_ipi_item
                }
                dados.append(item)
                
        except Exception as e:
            print(f"Erro ao ler arquivo {arq}: {e}")
            pass
            
    # Criar DataFrame na ordem exata solicitada
    colunas_ordem = [
        'Mês', 'Ano', 'Chave Acesso NFe', 'Inscrição Destinatário', 'Inscrição Emitente', 
        'Razão Social Emitente', 'Cnpj Emitente', 'UF Emitente', 'Nr NFe', 'Série', 'Data NFe', 
        'BC ICMS Total', 'ICMS Total', 'BC ST Total', 'ICMS ST Total', 'Desc Total', 'IPI Total', 
        'Total Produtos', 'Total NFe', 'Descrição Produto NFe', 'NCM na NFe', 'CST', 'CFOP NFe', 
        'Qtde', 'Unid', 'Vr Unit', 'Vr Total', 'Desconto Item', 'Base de Cálculo ICMS', 
        'Aliq ICMS', 'Vr ICMS', 'Aliq IPI', 'Vr IPI'
    ]
    
    df = pd.DataFrame(dados)
    
    # Reordenar colunas (se existirem dados)
    if not df.empty:
        # Garante que todas colunas existem mesmo se vazias
        for col in colunas_ordem:
            if col not in df.columns:
                df[col] = ""
        df = df[colunas_ordem]
        
    return df

# --- ROTAS (MANTIDAS IGUAIS) ---
@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <html>
        <head>
            <title>Extrator XML - Personalizado</title>
            <style>
                body { font-family: sans-serif; text-align: center; padding: 50px; background: #2c3e50; color: white;}
                .box { background: white; color: #333; padding: 40px; border-radius: 8px; display: inline-block; }
                button { background: #27ae60; color: white; border: none; padding: 15px 30px; font-size: 16px; cursor: pointer; border-radius: 5px; }
            </style>
        </head>
        <body>
            <div class="box">
                <h1>Extrator de XML 3.0 🚀</h1>
                <p>Relatório com layout personalizado.</p>
                <form action="/upload" method="post" enctype="multipart/form-data">
                    <input type="file" name="file" accept=".zip" required>
                    <br><br>
                    <button type="submit">Gerar Relatório Excel</button>
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
        
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
    except:
        return HTMLResponse("<h1>Erro: Arquivo ZIP inválido.</h1>")
        
    df = processar_xmls(temp_dir)
    
    output_file = "Relatorio_Personalizado.xlsx"
    df.to_excel(output_file, index=False)
    
    return FileResponse(output_file, filename="Relatorio_Personalizado.xlsx", media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
