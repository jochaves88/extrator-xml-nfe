import os
import shutil
import zipfile
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse

app = FastAPI()

# --- LÓGICA DE EXTRAÇÃO ROBUSTA ---
ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

def pegar_valor(no, caminho, tipo=str):
    """Busca valor de forma segura. Se der erro, retorna vazio ou zero."""
    if no is None: return tipo(0) if tipo in [float, int] else ""
    try:
        r = no.find(caminho, ns)
        if r is not None and r.text:
            return tipo(r.text)
        return tipo(0) if tipo in [float, int] else ""
    except:
        return tipo(0) if tipo in [float, int] else ""

def processar_xmls(pasta_xml):
    # Busca recursiva por .xml ou .XML
    arquivos = glob.glob(f"{pasta_xml}/**/*.xml", recursive=True)
    arquivos += glob.glob(f"{pasta_xml}/**/*.XML", recursive=True)
    
    dados = []
    print(f"Total de arquivos encontrados: {len(arquivos)}") # Log para o Portainer
    
    for arq in arquivos:
        try:
            tree = ET.parse(arq)
            root = tree.getroot()
            
            # Ajuste para encontrar a tag correta (NFe normal ou NFeProcessada)
            if root.tag.endswith('nfeProc'):
                inf_nfe = root.find('nfe:NFe/nfe:infNFe', ns)
            else:
                inf_nfe = root.find('nfe:infNFe', ns)
            
            if inf_nfe is None:
                print(f"ALERTA: Arquivo ignorado (não parece NFe): {arq}")
                continue

            # --- BLOCOS DE DADOS ---
            ide = inf_nfe.find('nfe:ide', ns)
            emit = inf_nfe.find('nfe:emit', ns)
            dest = inf_nfe.find('nfe:dest', ns)
            total = inf_nfe.find('nfe:total/nfe:ICMSTot', ns)
            
            # Tratamento da Chave
            chave = pegar_valor(root.find('nfe:protNFe/nfe:infProt', ns), 'nfe:chNFe')
            if not chave: 
                chave = inf_nfe.attrib.get('Id', '')[3:]

            # Tratamento da Data (Emissão ou Saída)
            data_raw = pegar_valor(ide, 'nfe:dhEmi')
            if not data_raw: data_raw = pegar_valor(ide, 'nfe:dEmi')
            data_nfe = data_raw[:10]
            
            # --- LOOP DOS ITENS (PRODUTOS) ---
            dets = inf_nfe.findall('nfe:det', ns)
            for det in dets:
                prod = det.find('nfe:prod', ns)
                imposto = det.find('nfe:imposto', ns)
                
                # Buscando impostos (ICMS, IPI, PIS, COFINS) varrendo os filhos
                v_icms = 0.0
                v_ipi = 0.0
                v_pis = 0.0
                v_cofins = 0.0
                
                if imposto is not None:
                    # ICMS (pode estar dentro de ICMS00, ICMS20, etc...)
                    icms_node = imposto.find('nfe:ICMS', ns)
                    if icms_node:
                        for child in icms_node: # Varre qualquer filho (ICMS00, CSOSN101...)
                            val = child.find('nfe:vICMS', ns)
                            if val is not None: v_icms = float(val.text)
                    
                    # IPI
                    ipi_node = imposto.find('nfe:IPI', ns)
                    if ipi_node:
                        ipitrib = ipi_node.find('nfe:IPITrib', ns)
                        if ipitrib:
                            val = ipitrib.find('nfe:vIPI', ns)
                            if val is not None: v_ipi = float(val.text)

                    # PIS
                    pis_node = imposto.find('nfe:PIS', ns)
                    if pis_node:
                        for child in pis_node:
                            val = child.find('nfe:vPIS', ns)
                            if val is not None: v_pis = float(val.text)

                    # COFINS
                    cofins_node = imposto.find('nfe:COFINS', ns)
                    if cofins_node:
                        for child in cofins_node:
                            val = child.find('nfe:vCOFINS', ns)
                            if val is not None: v_cofins = float(val.text)

                item = {
                    'Chave Acesso': "'" + chave,
                    'Numero NFe': pegar_valor(ide, 'nfe:nNF'),
                    'Serie': pegar_valor(ide, 'nfe:serie'),
                    'Data Emissao': data_nfe,
                    'Natureza Op': pegar_valor(ide, 'nfe:natOp'),
                    
                    # Emitente
                    'CNPJ Emitente': pegar_valor(emit, 'nfe:CNPJ'),
                    'Nome Emitente': pegar_valor(emit, 'nfe:xNome'),
                    'UF Emitente': pegar_valor(emit, 'nfe:enderEmit/nfe:UF'),
                    
                    # Destinatario
                    'CNPJ Destinatario': pegar_valor(dest, 'nfe:CNPJ') or pegar_valor(dest, 'nfe:CPF'),
                    'Nome Destinatario': pegar_valor(dest, 'nfe:xNome'),
                    'UF Destinatario': pegar_valor(dest, 'nfe:enderDest/nfe:UF'),
                    
                    # Produto
                    'Codigo Produto': pegar_valor(prod, 'nfe:cProd'),
                    'Descricao': pegar_valor(prod, 'nfe:xProd'),
                    'NCM': pegar_valor(prod, 'nfe:NCM'),
                    'CFOP': pegar_valor(prod, 'nfe:CFOP'),
                    'Unidade': pegar_valor(prod, 'nfe:uCom'),
                    'Quantidade': pegar_valor(prod, 'nfe:qCom', float),
                    'Valor Unitario': pegar_valor(prod, 'nfe:vUnCom', float),
                    'Valor Total Item': pegar_valor(prod, 'nfe:vProd', float),
                    
                    # Impostos Item
                    'Valor ICMS': v_icms,
                    'Valor IPI': v_ipi,
                    'Valor PIS': v_pis,
                    'Valor COFINS': v_cofins,
                    
                    # Totais Nota
                    'Total Nota': pegar_valor(total, 'nfe:vNF', float)
                }
                dados.append(item)
        except Exception as e:
            # AQUI ESTA A MAGICA: Se der erro, ele avisa qual arquivo foi
            print(f"ERRO CRITICO no arquivo {arq}: {e}")
            pass
            
    df = pd.DataFrame(dados)
    return df

# --- ROTAS DO SITE ---

@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <html>
        <head>
            <title>Extrator PRO V2</title>
            <style>
                body { font-family: 'Segoe UI', sans-serif; text-align: center; padding: 50px; background: #2c3e50; color: white; }
                .box { background: white; color: #333; padding: 40px; border-radius: 10px; display: inline-block; max-width: 500px; }
                button { background: #e67e22; color: white; border: none; padding: 15px 30px; font-size: 18px; cursor: pointer; border-radius: 5px; width: 100%; }
                button:hover { background: #d35400; }
                input { margin-bottom: 20px; padding: 10px; width: 100%; box-sizing: border-box;}
            </style>
        </head>
        <body>
            <div class="box">
                <h1>Extrator de XML 2.0 ⚡</h1>
                <p>Extração completa com impostos e detalhes.</p>
                <form action="/upload" method="post" enctype="multipart/form-data">
                    <input type="file" name="file" accept=".zip" required>
                    <br>
                    <button type="submit">Processar Arquivos</button>
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
        return HTMLResponse("<h1>Erro: O arquivo enviado não é um ZIP válido.</h1>")
        
    df = processar_xmls(temp_dir)
    
    if df.empty:
        return HTMLResponse("<h1>Erro: Nenhuma nota encontrada ou erro na leitura. Verifique os Logs no Portainer.</h1>")

    output_file = "Relatorio_Completo.xlsx"
    df.to_excel(output_file, index=False)
    
    return FileResponse(output_file, filename="Relatorio_Completo.xlsx", media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
