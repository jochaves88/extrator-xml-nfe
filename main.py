import os
import shutil
import zipfile
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse

app = FastAPI()

# --- CONFIGURAÇÃO E LÓGICA DE EXTRAÇÃO ---
ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

def pegar_valor(no, caminho, tipo=str):
    """Busca valor de forma segura. Retorna 0.0 para numeros se nao encontrar."""
    if no is None: return tipo(0) if tipo in [float, int] else ""
    try:
        r = no.find(caminho, ns)
        if r is not None and r.text:
            # Substitui virgula por ponto se for numero, para evitar erro de conversao
            valor_txt = r.text.replace(',', '.')
            return tipo(valor_txt)
        return tipo(0) if tipo in [float, int] else ""
    except:
        return tipo(0) if tipo in [float, int] else ""

def processar_xmls(pasta_xml):
    # Busca recursiva por .xml ou .XML
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
            total_icms = inf_nfe.find('nfe:total/nfe:ICMSTot', ns)
            
            # --- DADOS GERAIS DA NOTA ---
            chave = pegar_valor(root.find('nfe:protNFe/nfe:infProt', ns), 'nfe:chNFe')
            if not chave: chave = inf_nfe.attrib.get('Id', '')[3:]
            
            data_raw = pegar_valor(ide, 'nfe:dhEmi')
            if not data_raw: data_raw = pegar_valor(ide, 'nfe:dEmi')
            data_nfe = data_raw[:10] # Formato AAAA-MM-DD
            
            ano = data_nfe[:4]
            mes = data_nfe[5:7]

            # --- LOOP DE PRODUTOS ---
            dets = inf_nfe.findall('nfe:det', ns)
            for det in dets:
                prod = det.find('nfe:prod', ns)
                imposto = det.find('nfe:imposto', ns)
                
                cst_csosn = ""
                bc_icms_item = 0.0
                aliq_icms_item = 0.0
                vr_icms_item = 0.0
                aliq_ipi_item = 0.0
                vr_ipi_item = 0.0
                
                if imposto is not None:
                    icms_node = imposto.find('nfe:ICMS', ns)
                    if icms_node:
                        for child in icms_node:
                            cst_csosn = pegar_valor(child, 'nfe:CST')
                            if not cst_csosn: cst_csosn = pegar_valor(child, 'nfe:CSOSN')
                            
                            bc_icms_item = pegar_valor(child, 'nfe:vBC', float)
                            aliq_icms_item = pegar_valor(child, 'nfe:pICMS', float)
                            vr_icms_item = pegar_valor(child, 'nfe:vICMS', float)

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
                    
                    'BC ICMS Total': pegar_valor(total_icms, 'nfe:vBC', float),
                    'ICMS Total': pegar_valor(total_icms, 'nfe:vICMS', float),
                    'BC ST Total': pegar_valor(total_icms, 'nfe:vBCST', float),
                    'ICMS ST Total': pegar_valor(total_icms, 'nfe:vST', float),
                    'Desc Total': pegar_valor(total_icms, 'nfe:vDesc', float),
                    'IPI Total': pegar_valor(total_icms, 'nfe:vIPI', float),
                    'Total Produtos': pegar_valor(total_icms, 'nfe:vProd', float),
                    'Total NFe': pegar_valor(total_icms, 'nfe:vNF', float),
                    
                    'Descrição Produto NFe': pegar_valor(prod, 'nfe:xProd'),
                    'NCM na NFe': pegar_valor(prod, 'nfe:NCM'),
                    'CST': cst_csosn,
                    'CFOP NFe': pegar_valor(prod, 'nfe:CFOP'),
                    'Qtde': pegar_valor(prod, 'nfe:qCom', float),
                    'Unid': pegar_valor(prod, 'nfe:uCom'),
                    'Vr Unit': pegar_valor(prod, 'nfe:vUnCom', float),
                    'Vr Total': pegar_valor(prod, 'nfe:vProd', float),
                    'Desconto Item': pegar_valor(prod, 'nfe:vDesc', float),
                    
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
    
    if not df.empty:
        # Garante que todas colunas existem
        for col in colunas_ordem:
            if col not in df.columns: df[col] = ""
        df = df[colunas_ordem]
        
    return df

# --- INTERFACE VISUAL (HTML/CSS/JS) ---
@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html lang="pt-br">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Extrator Fiscal Pro</title>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
        <style>
            body { font-family: 'Inter', sans-serif; background: #f3f4f6; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; color: #1f2937; }
            .card { background: white; padding: 48px; border-radius: 20px; box-shadow: 0 20px 25px -5px rgba(0,0,0,0.1), 0 10px 10px -5px rgba(0,0,0,0.04); width: 100%; max-width: 500px; text-align: center; }
            h1 { color: #111827; margin: 0 0 12px 0; font-size: 26px; letter-spacing: -0.025em; }
            p.subtitle { color: #6b7280; margin: 0 0 32px 0; font-size: 15px; }
            
            .drop-zone { border: 2px dashed #e5e7eb; border-radius: 12px; padding: 40px 20px; cursor: pointer; transition: all 0.2s ease; position: relative; background: #f9fafb; }
            .drop-zone:hover, .drop-zone.dragover { border-color: #3b82f6; background: #eff6ff; }
            .drop-zone input { position: absolute; width: 100%; height: 100%; top: 0; left: 0; opacity: 0; cursor: pointer; }
            
            .icon-folder { font-size: 48px; margin-bottom: 12px; display: block; }
            .drop-text { font-weight: 500; color: #4b5563; }
            
            .file-info { display: none; margin-top: 15px; background: #dbeafe; color: #1e40af; padding: 8px 16px; border-radius: 9999px; font-size: 14px; font-weight: 600; display: inline-flex; align-items: center; }
            
            .btn { background: #2563eb; color: white; border: none; padding: 16px; width: 100%; border-radius: 10px; font-weight: 600; font-size: 16px; cursor: pointer; margin-top: 24px; transition: background 0.2s; display: flex; justify-content: center; align-items: center; box-shadow: 0 4px 6px -1px rgba(37, 99, 235, 0.2); }
            .btn:hover { background: #1d4ed8; }
            .btn:disabled { background: #9ca3af; cursor: not-allowed; box-shadow: none; }
            
            .status { margin-top: 20px; font-size: 14px; min-height: 20px; font-weight: 500; }
            
            /* Loader Animation */
            .loader { border: 3px solid rgba(255,255,255,0.3); border-top: 3px solid white; border-radius: 50%; width: 20px; height: 20px; animation: spin 1s linear infinite; margin-right: 10px; display: none; }
            @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        </style>
    </head>
    <body>
        <div class="card">
            <h1>Extrator XML NFe</h1>
            <p class="subtitle">Transforme seus XMLs em Excel em segundos</p>
            
            <form id="uploadForm">
                <div class="drop-zone" id="dropZone">
                    <span class="icon-folder">📂</span>
                    <div id="instructionText">
                        <div class="drop-text">Arraste seu ZIP aqui</div>
                        <div style="font-size: 13px; color: #9ca3af; margin-top: 4px;">ou clique para selecionar</div>
                    </div>
                    <div id="fileDisplay" style="display:none;">
                        <span class="file-info" id="fileNameBadge"></span>
                    </div>
                    <input type="file" name="file" accept=".zip" required id="fileInput">
                </div>
                
                <button type="submit" class="btn" id="btnSubmit">
                    <span class="loader" id="loader"></span>
                    <span id="btnText">Processar Arquivos</span>
                </button>
            </form>
            <div class="status" id="statusMsg"></div>
        </div>

        <script>
            const dropZone = document.getElementById('dropZone');
            const fileInput = document.getElementById('fileInput');
            const fileNameBadge = document.getElementById('fileNameBadge');
            const fileDisplay = document.getElementById('fileDisplay');
            const instructionText = document.getElementById('instructionText');
            const form = document.getElementById('uploadForm');
            const btn = document.getElementById('btnSubmit');
            const loader = document.getElementById('loader');
            const btnText = document.getElementById('btnText');
            const statusMsg = document.getElementById('statusMsg');

            // Drag and Drop Effects
            dropZone.addEventListener('dragover', (e) => { e.preventDefault(); dropZone.classList.add('dragover'); });
            dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'); });
            dropZone.addEventListener('drop', (e) => {
                e.preventDefault();
                dropZone.classList.remove('dragover');
                if(e.dataTransfer.files.length) {
                    fileInput.files = e.dataTransfer.files;
                    updateFileInfo();
                }
            });

            fileInput.addEventListener('change', updateFileInfo);

            function updateFileInfo() {
                if(fileInput.files.length) {
                    instructionText.style.display = 'none';
                    fileDisplay.style.display = 'block';
                    fileNameBadge.textContent = '📄 ' + fileInput.files[0].name;
                    statusMsg.textContent = '';
                }
            }

            form.addEventListener('submit', async (e) => {
                e.preventDefault();
                if(!fileInput.files.length) {
                    statusMsg.style.color = '#ef4444';
                    statusMsg.textContent = "Por favor, selecione um arquivo ZIP primeiro.";
                    return;
                }

                // UI Loading State
                btn.disabled = true;
                btnText.textContent = "Processando...";
                loader.style.display = 'inline-block';
                statusMsg.textContent = "";

                const formData = new FormData();
                formData.append("file", fileInput.files[0]);

                try {
                    const response = await fetch('/upload', { method: 'POST', body: formData });
                    
                    if (response.ok) {
                        const blob = await response.blob();
                        const url = window.URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url;
                        a.download = "Relatorio_Notas.xlsx";
                        document.body.appendChild(a);
                        a.click();
                        a.remove();
                        statusMsg.style.color = '#059669';
                        statusMsg.textContent = "Sucesso! O download começou.";
                    } else {
                        statusMsg.style.color = '#ef4444';
                        statusMsg.textContent = "Erro ao processar. Verifique se o ZIP contém XMLs válidos.";
                    }
                } catch (err) {
                    statusMsg.style.color = '#ef4444';
                    statusMsg.textContent = "Erro de conexão com o servidor.";
                }

                // Reset UI
                btn.disabled = false;
                btnText.textContent = "Processar Arquivos";
                loader.style.display = 'none';
            });
        </script>
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
        return HTMLResponse("Erro: Arquivo não é um ZIP válido", status_code=400)
        
    df = processar_xmls(temp_dir)
    
    output_file = "Relatorio_Notas.xlsx"
    df.to_excel(output_file, index=False)
    
    if df.empty:
        # Retorna uma planilha vazia com cabeçalhos se não achar nada, para nao quebrar o front
        pass 

    return FileResponse(output_file, filename="Relatorio_Notas.xlsx", media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
