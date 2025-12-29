import os
import shutil
import zipfile
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from typing import List

app = FastAPI()

# --- CONFIGURAÇÃO ---
ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
temp_dir = "temp_files"
output_filename = "Relatorio_Notas.xlsx"

def formatar_moeda(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def pegar_valor(no, caminho, tipo=str):
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
    erros = []
    
    for arq in arquivos:
        try:
            tree = ET.parse(arq)
            root = tree.getroot()
            
            if root.tag.endswith('nfeProc'):
                inf_nfe = root.find('nfe:NFe/nfe:infNFe', ns)
            else:
                inf_nfe = root.find('nfe:infNFe', ns)
            
            if inf_nfe is None: continue

            # --- DADOS ---
            ide = inf_nfe.find('nfe:ide', ns)
            emit = inf_nfe.find('nfe:emit', ns)
            dest = inf_nfe.find('nfe:dest', ns)
            total_icms = inf_nfe.find('nfe:total/nfe:ICMSTot', ns)
            
            chave = pegar_valor(root.find('nfe:protNFe/nfe:infProt', ns), 'nfe:chNFe')
            if not chave: chave = inf_nfe.attrib.get('Id', '')[3:]
            
            data_raw = pegar_valor(ide, 'nfe:dhEmi') or pegar_valor(ide, 'nfe:dEmi')
            data_nfe = data_raw[:10]
            ano, mes = data_nfe[:4], data_nfe[5:7]

            # --- LOOP ITENS ---
            dets = inf_nfe.findall('nfe:det', ns)
            for det in dets:
                prod = det.find('nfe:prod', ns)
                imposto = det.find('nfe:imposto', ns)
                
                # ... (Lógica de impostos igual anterior) ...
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
                            cst_csosn = pegar_valor(child, 'nfe:CST') or pegar_valor(child, 'nfe:CSOSN')
                            bc_icms_item = pegar_valor(child, 'nfe:vBC', float)
                            aliq_icms_item = pegar_valor(child, 'nfe:pICMS', float)
                            vr_icms_item = pegar_valor(child, 'nfe:vICMS', float)
                    ipi_node = imposto.find('nfe:IPI', ns)
                    if ipi_node:
                        ipitrib = ipi_node.find('nfe:IPITrib', ns)
                        if ipitrib:
                            aliq_ipi_item = pegar_valor(ipitrib, 'nfe:pIPI', float)
                            vr_ipi_item = pegar_valor(ipitrib, 'nfe:vIPI', float)

                item = {
                    'Mês': mes, 'Ano': ano, 'Chave': chave,
                    'Emitente': pegar_valor(emit, 'nfe:xNome'),
                    'CNPJ Emitente': pegar_valor(emit, 'nfe:CNPJ'),
                    'Nr NFe': pegar_valor(ide, 'nfe:nNF'),
                    'Data': data_nfe,
                    'Total NFe': pegar_valor(total_icms, 'nfe:vNF', float),
                    'Produto': pegar_valor(prod, 'nfe:xProd'),
                    'NCM': pegar_valor(prod, 'nfe:NCM'),
                    'CFOP': pegar_valor(prod, 'nfe:CFOP'),
                    'Qtde': pegar_valor(prod, 'nfe:qCom', float),
                    'Vr Total Item': pegar_valor(prod, 'nfe:vProd', float),
                    # ... (Adicione os outros campos aqui se quiser na planilha final) ...
                }
                dados.append(item)
        except Exception as e:
            erros.append(f"Erro em {os.path.basename(arq)}: {str(e)}")

    df = pd.DataFrame(dados)
    
    # --- GERAÇÃO DE ESTATÍSTICAS (PROVA REAL) ---
    stats = {}
    if not df.empty:
        # 1. Totais Gerais
        qtd_arquivos = len(arquivos)
        qtd_linhas_produtos = len(df)
        # Conta notas únicas pela chave (remove duplicatas de produtos para contar notas)
        notas_unicas = df.drop_duplicates(subset=['Chave'])
        qtd_notas = len(notas_unicas)
        valor_total_notas = notas_unicas['Total NFe'].sum()
        
        # 2. Agrupamento por Mês/Ano (usando as notas únicas para não somar o total da nota várias vezes)
        por_periodo = notas_unicas.groupby(['Ano', 'Mês']).agg(
            Qtd_Notas=('Chave', 'count'),
            Valor_Total=('Total NFe', 'sum')
        ).reset_index().to_dict('records')
        
        # 3. Top Emitentes
        top_emitentes = notas_unicas['Emitente'].value_counts().head(5).to_dict()

        stats = {
            'sucesso': True,
            'qtd_arquivos_lidos': qtd_arquivos,
            'qtd_notas_unicas': qtd_notas,
            'qtd_produtos_total': qtd_linhas_produtos,
            'valor_total_geral': valor_total_notas,
            'periodos': por_periodo,
            'top_emitentes': top_emitentes,
            'erros': erros
        }
    else:
        stats = {'sucesso': False, 'msg': 'Nenhum dado encontrado nos XMLs.'}

    return df, stats

# --- ROTAS ---

@app.get("/download")
async def download_excel():
    file_path = os.path.join(temp_dir, output_filename)
    if os.path.exists(file_path):
        return FileResponse(file_path, filename=output_filename, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    return {"error": "Arquivo não encontrado"}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
    
    zip_path = os.path.join(temp_dir, "arquivo.zip")
    with open(zip_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
        
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
    except:
        return JSONResponse({"sucesso": False, "msg": "Arquivo ZIP inválido"}, status_code=400)
        
    df, stats = processar_xmls(temp_dir)
    
    # Salva o Excel para download posterior
    excel_path = os.path.join(temp_dir, output_filename)
    df.to_excel(excel_path, index=False)
    
    return JSONResponse(stats)

@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html lang="pt-br">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Auditoria Fiscal XML</title>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
        <style>
            body { font-family: 'Inter', sans-serif; background: #f8fafc; color: #1e293b; margin: 0; padding: 20px; }
            .container { max-width: 900px; margin: 0 auto; }
            
            /* Upload Card */
            .card { background: white; padding: 40px; border-radius: 16px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); text-align: center; margin-bottom: 30px; }
            .drop-zone { border: 2px dashed #cbd5e1; border-radius: 12px; padding: 30px; cursor: pointer; transition: 0.3s; background: #f1f5f9; position: relative;}
            .drop-zone:hover { border-color: #3b82f6; background: #e0f2fe; }
            .drop-zone input { position: absolute; width: 100%; height: 100%; top: 0; left: 0; opacity: 0; cursor: pointer; }
            .btn { background: #3b82f6; color: white; border: none; padding: 15px 30px; border-radius: 8px; font-weight: 600; font-size: 16px; cursor: pointer; margin-top: 20px; width: 100%; }
            .btn:disabled { background: #94a3b8; cursor: not-allowed; }
            
            /* Dashboard Styles (Escondido inicialmente) */
            #dashboard { display: none; }
            .grid-stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
            .stat-card { background: white; padding: 20px; border-radius: 12px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); border-left: 5px solid #3b82f6; }
            .stat-value { font-size: 24px; font-weight: 700; color: #0f172a; margin-top: 5px; }
            .stat-label { font-size: 14px; color: #64748b; font-weight: 600; }
            
            /* Tabela */
            .table-container { background: white; border-radius: 12px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); overflow-x: auto; }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; }
            th, td { padding: 12px; text-align: left; border-bottom: 1px solid #e2e8f0; }
            th { background: #f8fafc; color: #475569; font-weight: 600; }
            
            .btn-download { background: #10b981; margin-top: 20px; display: block; text-decoration: none; text-align: center; padding: 15px; border-radius: 8px; color: white; font-weight: bold; font-size: 18px; }
            .btn-download:hover { background: #059669; }
            
            /* Loader */
            .loader { border: 3px solid rgba(255,255,255,0.3); border-top: 3px solid white; border-radius: 50%; width: 20px; height: 20px; animation: spin 1s linear infinite; display: none; vertical-align: middle; margin-right: 10px; }
            @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="card" id="uploadCard">
                <h1 style="margin-top:0">Auditor Fiscal XML 📊</h1>
                <p style="color:#64748b">Arraste seu ZIP para ver a Prova Real</p>
                <form id="uploadForm">
                    <div class="drop-zone" id="dropZone">
                        <span style="font-size:40px">📂</span>
                        <div id="fileInfo">Solte o arquivo ZIP aqui</div>
                        <input type="file" name="file" accept=".zip" required id="fileInput">
                    </div>
                    <button type="submit" class="btn" id="btnSubmit">
                        <span class="loader" id="loader"></span> <span id="btnText">Processar Dados</span>
                    </button>
                </form>
                <div id="msgErro" style="color:red; margin-top:15px"></div>
            </div>

            <div id="dashboard">
                <h2>📋 Resumo da Auditoria (Prova Real)</h2>
                
                <div class="grid-stats">
                    <div class="stat-card">
                        <div class="stat-label">Total de Notas</div>
                        <div class="stat-value" id="resQtdNotas">0</div>
                    </div>
                    <div class="stat-card" style="border-color: #10b981;">
                        <div class="stat-label">Valor Total (R$)</div>
                        <div class="stat-value" id="resValorTotal" style="color:#059669">R$ 0,00</div>
                    </div>
                    <div class="stat-card" style="border-color: #f59e0b;">
                        <div class="stat-label">Itens/Produtos</div>
                        <div class="stat-value" id="resQtdItens">0</div>
                    </div>
                    <div class="stat-card" style="border-color: #6366f1;">
                        <div class="stat-label">Arquivos Processados</div>
                        <div class="stat-value" id="resQtdArquivos">0</div>
                    </div>
                </div>

                <div class="table-container">
                    <h3>📅 Detalhamento por Mês</h3>
                    <table>
                        <thead>
                            <tr>
                                <th>Ano</th>
                                <th>Mês</th>
                                <th>Qtd Notas</th>
                                <th>Valor Total</th>
                            </tr>
                        </thead>
                        <tbody id="tabelaMeses">
                            </tbody>
                    </table>
                </div>

                <a href="/download" class="btn-download">
                    📥 Tudo Certo! Baixar Relatório Excel
                </a>
                
                <br>
                <button onclick="location.reload()" style="background:none; border:none; color:#64748b; cursor:pointer; text-decoration:underline; width:100%">Processar outro arquivo</button>
            </div>
        </div>

        <script>
            const form = document.getElementById('uploadForm');
            const fileInput = document.getElementById('fileInput');
            const fileInfo = document.getElementById('fileInfo');
            const dashboard = document.getElementById('dashboard');
            const uploadCard = document.getElementById('uploadCard');
            const btn = document.getElementById('btnSubmit');
            const loader = document.getElementById('loader');
            const msgErro = document.getElementById('msgErro');

            fileInput.addEventListener('change', () => {
                if(fileInput.files.length) fileInfo.textContent = "📄 " + fileInput.files[0].name;
            });

            const formatBRL = (val) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(val);

            form.addEventListener('submit', async (e) => {
                e.preventDefault();
                if(!fileInput.files.length) return;

                btn.disabled = true;
                loader.style.display = 'inline-block';
                msgErro.textContent = "";

                const formData = new FormData();
                formData.append("file", fileInput.files[0]);

                try {
                    const res = await fetch('/upload', { method: 'POST', body: formData });
                    const data = await res.json();

                    if(data.sucesso) {
                        // Preencher Dashboard
                        document.getElementById('resQtdNotas').textContent = data.qtd_notas_unicas;
                        document.getElementById('resValorTotal').textContent = formatBRL(data.valor_total_geral);
                        document.getElementById('resQtdItens').textContent = data.qtd_produtos_total;
                        document.getElementById('resQtdArquivos').textContent = data.qtd_arquivos_lidos;

                        // Preencher Tabela
                        const tbody = document.getElementById('tabelaMeses');
                        tbody.innerHTML = '';
                        data.periodos.forEach(p => {
                            const tr = `<tr>
                                <td>${p.Ano}</td>
                                <td>${p.Mês}</td>
                                <td>${p.Qtd_Notas}</td>
                                <td>${formatBRL(p.Valor_Total)}</td>
                            </tr>`;
                            tbody.innerHTML += tr;
                        });

                        // Trocar tela
                        uploadCard.style.display = 'none';
                        dashboard.style.display = 'block';
                    } else {
                        msgErro.textContent = data.msg || "Erro ao processar.";
                    }
                } catch (err) {
                    msgErro.textContent = "Erro de conexão.";
                }

                btn.disabled = false;
                loader.style.display = 'none';
            });
        </script>
    </body>
    </html>
    """
