import os
import shutil
import zipfile
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from sqlalchemy import create_engine, Column, String, Float, Date, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

app = FastAPI()

# --- 1. CONFIGURAÇÃO DO BANCO DE DADOS ---
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class NFe(Base):
    __tablename__ = "notas_fiscais"
    # Chave composta (Chave NFe + Numero Item) para evitar duplicidade de itens
    chave_item = Column(String, primary_key=True, index=True) 
    chave_acesso = Column(String)
    mes = Column(String)
    ano = Column(String)
    data_emissao = Column(Date)
    valor_total_nota = Column(Float) # Valor total da NF (cabeçalho)
    valor_item = Column(Float)       # Valor deste item específico
    dados_json = Column(String)      # Dicionário completo para o Excel

Base.metadata.create_all(bind=engine)

# --- 2. FUNÇÕES XML OTIMIZADAS ---
# Namespaces comuns em NFe (ajuda a evitar erros de leitura)
ns_map = {
    'nfe': 'http://www.portalfiscal.inf.br/nfe',
    'default': 'http://www.portalfiscal.inf.br/nfe' 
}

def pegar_valor(no, caminho, tipo=str):
    """Busca valor tentando com e sem namespace para garantir"""
    if no is None: return tipo(0) if tipo in [float, int] else ""
    
    # Tenta encontrar com o namespace padrão
    r = no.find(caminho, ns_map)
    # Se não achar, tenta sem namespace (alguns XMLs antigos vêm assim)
    if r is None:
        caminho_sem_ns = caminho.replace('nfe:', '')
        r = no.find(caminho_sem_ns)
    
    if r is not None and r.text:
        val = r.text.replace(',', '.')
        try:
            return tipo(val)
        except:
            return tipo(0) if tipo in [float, int] else ""
    return tipo(0) if tipo in [float, int] else ""

def formatar_data(data_obj):
    """Retorna data no formato BR (dd/mm/aaaa) para o Excel"""
    return data_obj.strftime('%d/%m/%Y')

def extrair_dados_xml(arq):
    try:
        tree = ET.parse(arq)
        root = tree.getroot()
        
        # Ajuste para ler XMLs que começam com nfeProc ou direto NFe
        if 'nfeProc' in root.tag:
            inf_nfe = root.find('.//nfe:infNFe', ns_map)
        else:
            inf_nfe = root.find('nfe:infNFe', ns_map)
            
        if inf_nfe is None: return []

        # Grupos principais
        ide = inf_nfe.find('nfe:ide', ns_map)
        emit = inf_nfe.find('nfe:emit', ns_map)
        dest = inf_nfe.find('nfe:dest', ns_map)
        total = inf_nfe.find('.//nfe:ICMSTot', ns_map)
        
        # Identificação da Chave
        prot = root.find('.//nfe:infProt', ns_map)
        chave = pegar_valor(prot, 'nfe:chNFe')
        if not chave: 
            chave = inf_nfe.attrib.get('Id', '')[3:]

        # Datas
        data_raw = pegar_valor(ide, 'nfe:dhEmi') or pegar_valor(ide, 'nfe:dEmi')
        if len(data_raw) >= 10:
            data_nfe_dt = datetime.strptime(data_raw[:10], '%Y-%m-%d').date()
        else:
            data_nfe_dt = datetime.now().date() # Fallback

        valor_total_nf_float = pegar_valor(total, 'nfe:vNF', float)

        itens_db = []
        dets = inf_nfe.findall('nfe:det', ns_map)
        
        for i, det in enumerate(dets):
            prod = det.find('nfe:prod', ns_map)
            
            valor_item_float = pegar_valor(prod, 'nfe:vProd', float)
            
            # --- CRIAÇÃO DO DICIONÁRIO PARA O EXCEL ---
            # Aqui garantimos que Mês e Ano existam
            item_completo = {
                'Mês': str(data_nfe_dt.month).zfill(2),
                'Ano': str(data_nfe_dt.year),
                'Chave Acesso NFe': chave,
                'Inscrição Destinatário': pegar_valor(dest, 'nfe:IE'),
                'Inscrição Emitente': pegar_valor(emit, 'nfe:IE'),
                'Razão Social Emitente': pegar_valor(emit, 'nfe:xNome'),
                'Cnpj Emitente': pegar_valor(emit, 'nfe:CNPJ'),
                'UF Emitente': pegar_valor(emit, 'nfe:enderEmit/nfe:UF'),
                'Nr NFe': pegar_valor(ide, 'nfe:nNF'),
                'Série': pegar_valor(ide, 'nfe:serie'),
                'Data NFe': formatar_data(data_nfe_dt),
                
                # Valores Monetários
                'BC ICMS Total': pegar_valor(total, 'nfe:vBC', float),
                'ICMS Total': pegar_valor(total, 'nfe:vICMS', float),
                'BC ST Total': pegar_valor(total, 'nfe:vBCST', float),
                'ICMS ST Total': pegar_valor(total, 'nfe:vST', float),
                'Desc Total': pegar_valor(total, 'nfe:vDesc', float),
                'IPI Total': pegar_valor(total, 'nfe:vIPI', float),
                'Total Produtos': pegar_valor(total, 'nfe:vProd', float),
                'Total NFe': valor_total_nf_float,
                
                # Dados do Item
                'Descrição Produto NFe': pegar_valor(prod, 'nfe:xProd'),
                'NCM na NFe': pegar_valor(prod, 'nfe:NCM'),
                'CFOP NFe': pegar_valor(prod, 'nfe:CFOP'),
                'Qtde': pegar_valor(prod, 'nfe:qCom', float),
                'Unid': pegar_valor(prod, 'nfe:uCom'),
                'Vr Unit': pegar_valor(prod, 'nfe:vUnCom', float),
                'Vr Total': valor_item_float, # Valor deste item
                'Desconto Item': pegar_valor(prod, 'nfe:vDesc', float)
            }
            
            # Objeto Banco de Dados
            nfe_db = NFe(
                chave_item=f"{chave}-{i+1}", # Identificador Único do Item
                chave_acesso=chave,
                mes=str(data_nfe_dt.month).zfill(2),
                ano=str(data_nfe_dt.year),
                data_emissao=data_nfe_dt,
                valor_total_nota=valor_total_nf_float,
                valor_item=valor_item_float,
                dados_json=str(item_completo)
            )
            itens_db.append(nfe_db)
            
        return itens_db
    except Exception as e:
        print(f"Erro ao processar {arq}: {e}")
        return []

# --- 3. ROTAS E LÓGICA ---

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    temp_dir = "temp_files"
    if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
    
    zip_path = os.path.join(temp_dir, "arquivo.zip")
    with open(zip_path, "wb") as buffer: shutil.copyfileobj(file.file, buffer)
        
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref: zip_ref.extractall(temp_dir)
    except:
        return JSONResponse({"sucesso": False, "msg": "Arquivo inválido (não é ZIP)"}, 400)
    
    arquivos = glob.glob(f"{temp_dir}/**/*.xml", recursive=True)
    arquivos += glob.glob(f"{temp_dir}/**/*.XML", recursive=True)
    
    session = SessionLocal()
    contador = 0
    try:
        for arq in arquivos:
            itens = extrair_dados_xml(arq)
            for item in itens:
                session.merge(item)
                contador += 1
        session.commit()
    except Exception as e:
        session.rollback()
        return JSONResponse({"sucesso": False, "msg": f"Erro interno: {str(e)}"})
    finally:
        session.close()

    return JSONResponse({"sucesso": True, "msg": f"{contador} itens (produtos) processados!"})

@app.get("/dados-disponiveis")
async def get_dados():
    session = SessionLocal()
    try:
        anos = session.query(NFe.ano).distinct().order_by(NFe.ano).all()
        return {"anos": [a[0] for a in anos]}
    finally:
        session.close()

# ROTA 1: Gera o arquivo, salva no disco e retorna a PROVA REAL (JSON)
@app.post("/processar-relatorio")
async def processar_relatorio(anos: str = Form(...)):
    lista_anos = anos.split(',')
    session = SessionLocal()
    try:
        # Busca dados
        resultados = session.query(NFe).filter(NFe.ano.in_(lista_anos)).all()
        
        if not resultados:
            return JSONResponse({"sucesso": False, "msg": "Nenhum dado encontrado para este período."})

        dados_excel = []
        for row in resultados:
            d = eval(row.dados_json)
            dados_excel.append(d)
            
        df = pd.DataFrame(dados_excel)
        
        # --- DEFINIÇÃO ESTRITA DA ORDEM DAS COLUNAS ---
        colunas_ordem = [
            'Mês', 'Ano', 'Chave Acesso NFe', 'Inscrição Destinatário', 'Inscrição Emitente', 
            'Razão Social Emitente', 'Cnpj Emitente', 'UF Emitente', 'Nr NFe', 'Série', 'Data NFe', 
            'BC ICMS Total', 'ICMS Total', 'BC ST Total', 'ICMS ST Total', 'Desc Total', 'IPI Total', 
            'Total Produtos', 'Total NFe', 'Descrição Produto NFe', 'NCM na NFe', 'CFOP NFe', 
            'Qtde', 'Unid', 'Vr Unit', 'Vr Total', 'Desconto Item'
        ]
        
        # Cria colunas vazias se faltarem e Reordena
        for col in colunas_ordem:
            if col not in df.columns: df[col] = ""
        df = df[colunas_ordem] # AQUI A ORDEM É FORÇADA
        
        # Salva arquivo localmente
        output_file = "Relatorio_Final.xlsx"
        df.to_excel(output_file, index=False)
        
        # --- CÁLCULO DA PROVA REAL ---
        # 1. Soma da coluna 'Vr Total' (Soma de todos os itens/linhas do excel)
        soma_itens = df['Vr Total'].sum()
        
        # 2. Soma da coluna 'Total NFe' (Removendo duplicadas de Nota para não somar o total da nota varias vezes)
        # Usamos a chave de acesso para identificar notas únicas
        df_unicas = df.drop_duplicates(subset=['Chave Acesso NFe'])
        soma_notas = df_unicas['Total NFe'].sum()
        
        return JSONResponse({
            "sucesso": True,
            "prova_real_itens": f"R$ {soma_itens:,.2f}",
            "prova_real_notas": f"R$ {soma_notas:,.2f}",
            "qtd_linhas": len(df),
            "qtd_notas": len(df_unicas),
            "download_url": "/baixar-arquivo"
        })
        
    except Exception as e:
        return JSONResponse({"sucesso": False, "msg": f"Erro ao gerar: {str(e)}"})
    finally:
        session.close()

# ROTA 2: Apenas entrega o arquivo gerado
@app.get("/baixar-arquivo")
async def baixar_arquivo():
    file_path = "Relatorio_Final.xlsx"
    if os.path.exists(file_path):
        return FileResponse(file_path, filename="Relatorio_Fiscal_Consolidado.xlsx")
    return JSONResponse({"msg": "Arquivo não encontrado. Gere novamente."}, 404)

# --- FRONTEND ---
@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html lang="pt-br">
    <head>
        <meta charset="UTF-8">
        <title>Extrator Fiscal Pro</title>
        <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
        <style>
            body { font-family: 'Roboto', sans-serif; background: #eef2f6; padding: 20px; }
            .container { max-width: 800px; margin: 0 auto; display: grid; gap: 20px; }
            .card { background: white; padding: 25px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
            h2 { margin-top: 0; color: #1e293b; border-bottom: 2px solid #e2e8f0; padding-bottom: 10px; }
            
            /* Upload */
            .drop-zone { border: 2px dashed #94a3b8; padding: 30px; text-align: center; border-radius: 8px; cursor: pointer; background: #f8fafc; }
            .drop-zone:hover { border-color: #3b82f6; background: #eff6ff; }
            
            /* Filtros */
            .checkbox-group { display: flex; gap: 10px; margin: 15px 0; flex-wrap: wrap; }
            .tag-check { cursor: pointer; background: #f1f5f9; padding: 8px 15px; border-radius: 20px; border: 1px solid #cbd5e1; transition: 0.2s; }
            .tag-check:has(input:checked) { background: #3b82f6; color: white; border-color: #3b82f6; }
            input[type="checkbox"] { display: none; }
            
            /* Botão e Status */
            .btn { width: 100%; padding: 15px; background: #3b82f6; color: white; border: none; border-radius: 6px; font-size: 16px; font-weight: bold; cursor: pointer; }
            .btn:disabled { background: #cbd5e1; cursor: not-allowed; }
            .btn:hover:not(:disabled) { background: #2563eb; }
            
            /* Prova Real Box */
            #resultBox { display: none; background: #dcfce7; border: 1px solid #86efac; padding: 20px; border-radius: 8px; margin-top: 20px; }
            .resumo-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-top: 10px; }
            .resumo-item { background: white; padding: 10px; border-radius: 6px; text-align: center; }
            .resumo-label { font-size: 0.85em; color: #64748b; }
            .resumo-val { font-size: 1.2em; font-weight: bold; color: #059669; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="card">
                <h2>1. Importar XMLs</h2>
                <div class="drop-zone" id="dropZone">
                    📂 Clique ou arraste o arquivo ZIP aqui
                    <input type="file" id="fileInput" accept=".zip" style="display:none">
                </div>
                <div id="uploadStatus" style="margin-top:10px; text-align:center; font-weight:bold;"></div>
            </div>

            <div class="card">
                <h2>2. Gerar Relatório e Prova Real</h2>
                <p>Selecione os anos:</p>
                <div class="checkbox-group" id="yearsContainer">Carregando...</div>
                
                <button class="btn" id="btnGerar" onclick="gerarRelatorio()" disabled>Processar Relatório</button>
                
                <div id="resultBox">
                    <h3 style="margin:0; color:#166534">✅ Relatório Gerado com Sucesso!</h3>
                    <p>O download iniciará automaticamente.</p>
                    <div class="resumo-grid">
                        <div class="resumo-item">
                            <div class="resumo-label">Total das Notas (Cabeçalho)</div>
                            <div class="resumo-val" id="valNotas">R$ 0,00</div>
                            <small id="qtdNotas">0 notas</small>
                        </div>
                        <div class="resumo-item">
                            <div class="resumo-label">Soma dos Itens (Produtos)</div>
                            <div class="resumo-val" id="valItens">R$ 0,00</div>
                            <small id="qtdLinhas">0 linhas</small>
                        </div>
                    </div>
                    <a href="#" id="downloadLink" style="display:block; text-align:center; margin-top:15px; color:#166534">Caso não baixe, clique aqui</a>
                </div>
            </div>
        </div>

        <script>
            // --- UPLOAD ---
            const dropZone = document.getElementById('dropZone');
            const fileInput = document.getElementById('fileInput');
            dropZone.onclick = () => fileInput.click();
            fileInput.onchange = () => handleUpload(fileInput.files[0]);
            
            dropZone.ondragover = (e) => { e.preventDefault(); dropZone.style.background = '#eff6ff'; };
            dropZone.ondragleave = () => dropZone.style.background = '#f8fafc';
            dropZone.ondrop = (e) => { e.preventDefault(); handleUpload(e.dataTransfer.files[0]); };

            async function handleUpload(file) {
                if(!file) return;
                const fd = new FormData(); fd.append("file", file);
                document.getElementById('uploadStatus').innerText = "Enviando...";
                
                const res = await fetch('/upload', { method: 'POST', body: fd });
                const data = await res.json();
                document.getElementById('uploadStatus').innerText = data.msg;
                document.getElementById('uploadStatus').style.color = data.sucesso ? "green" : "red";
                loadYears();
            }

            // --- FILTROS ---
            async function loadYears() {
                const res = await fetch('/dados-disponiveis');
                const data = await res.json();
                const div = document.getElementById('yearsContainer');
                div.innerHTML = "";
                data.anos.forEach(ano => {
                    div.innerHTML += `<label class="tag-check"><input type="checkbox" value="${ano}" onchange="checkBtn()"> ${ano}</label>`;
                });
                if(data.anos.length === 0) div.innerHTML = "Nenhum dado importado.";
            }

            function checkBtn() {
                const check = document.querySelectorAll('input[type="checkbox"]:checked');
                document.getElementById('btnGerar').disabled = check.length === 0;
            }

            // --- GERAR RELATORIO E PROVA REAL ---
            async function gerarRelatorio() {
                const btn = document.getElementById('btnGerar');
                const box = document.getElementById('resultBox');
                const checked = document.querySelectorAll('input[type="checkbox"]:checked');
                const anos = Array.from(checked).map(c => c.value).join(',');
                
                btn.disabled = true;
                btn.innerText = "Calculando...";
                box.style.display = 'none';

                const fd = new FormData();
                fd.append('anos', anos);

                try {
                    const res = await fetch('/processar-relatorio', { method: 'POST', body: fd });
                    const data = await res.json();
                    
                    if(data.sucesso) {
                        // Preenche a Prova Real
                        document.getElementById('valNotas').innerText = data.prova_real_notas;
                        document.getElementById('qtdNotas').innerText = data.qtd_notas + " notas unicas";
                        
                        document.getElementById('valItens').innerText = data.prova_real_itens;
                        document.getElementById('qtdLinhas').innerText = data.qtd_linhas + " linhas";
                        
                        // Link Manual
                        document.getElementById('downloadLink').href = data.download_url;
                        
                        // Mostra Box
                        box.style.display = 'block';
                        
                        // Download Automatico
                        window.location.href = data.download_url;
                    } else {
                        alert("Erro: " + data.msg);
                    }
                } catch(e) {
                    alert("Erro de comunicação.");
                }
                
                btn.disabled = false;
                btn.innerText = "Processar Relatório";
            }
            
            loadYears();
        </script>
    </body>
    </html>
    """
