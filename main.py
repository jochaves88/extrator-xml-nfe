import os
import shutil
import zipfile
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from typing import List
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from sqlalchemy import create_engine, Column, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

app = FastAPI()

# --- CONFIGURAÇÃO DE PASTAS ---
TEMP_DIR = "temp_files"
REPORTS_DIR = "meus_relatorios"

for d in [TEMP_DIR, REPORTS_DIR]:
    if not os.path.exists(d): os.makedirs(d)

# --- BANCO DE DADOS ---
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./nfe_data.db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class NFe(Base):
    __tablename__ = "notas_fiscais_v6" # V6: Tabela nova para garantir os novos campos (CST, Aliquotas)
    chave_item = Column(String, primary_key=True, index=True)
    chave_acesso = Column(String)
    ano = Column(String)
    mes = Column(String)
    cnpj_emitente = Column(String)
    cnpj_destinatario = Column(String)
    dados_json = Column(String)

Base.metadata.create_all(bind=engine)

# --- FUNÇÕES ---
ns_map = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

def get_val(node, path, type_fn=str):
    if node is None: return type_fn(0) if type_fn in [float, int] else ""
    try:
        r = node.find(path, ns_map)
        if r is None: r = node.find(path.replace('nfe:', ''))
        if r is not None and r.text:
            return type_fn(r.text.replace(',', '.'))
    except: pass
    return type_fn(0) if type_fn in [float, int] else ""

def parse_xml(filepath):
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        if 'nfeProc' in root.tag: inf = root.find('.//nfe:infNFe', ns_map)
        else: inf = root.find('nfe:infNFe', ns_map)
        if inf is None: return []

        ide = inf.find('nfe:ide', ns_map)
        emit = inf.find('nfe:emit', ns_map)
        dest = inf.find('nfe:dest', ns_map)
        total = inf.find('.//nfe:ICMSTot', ns_map)
        
        prot = root.find('.//nfe:infProt', ns_map)
        chave = get_val(prot, 'nfe:chNFe') or inf.attrib.get('Id', '')[3:]
        
        dt_str = get_val(ide, 'nfe:dhEmi') or get_val(ide, 'nfe:dEmi')
        dt = datetime.now()
        if len(dt_str) >= 10: dt = datetime.strptime(dt_str[:10], '%Y-%m-%d')

        # Extração de totais (Cabeçalho)
        v_bc_tot = get_val(total, 'nfe:vBC', float)
        v_icms_tot = get_val(total, 'nfe:vICMS', float)
        v_bcst_tot = get_val(total, 'nfe:vBCST', float)
        v_st_tot = get_val(total, 'nfe:vST', float)
        v_desc_tot = get_val(total, 'nfe:vDesc', float)
        v_ipi_tot = get_val(total, 'nfe:vIPI', float)
        v_prod_tot = get_val(total, 'nfe:vProd', float)
        v_nf_tot = get_val(total, 'nfe:vNF', float)

        itens_db = []
        for i, det in enumerate(inf.findall('nfe:det', ns_map)):
            prod = det.find('nfe:prod', ns_map)
            imposto = det.find('nfe:imposto', ns_map)
            
            # --- LÓGICA AVANÇADA PARA CST E IMPOSTOS DO ITEM ---
            # CST pode estar em vários lugares (ICMS00, ICMS10, CSOSN101, etc)
            # Buscamos recursivamente (.//) dentro da tag imposto
            cst = get_val(imposto, './/nfe:CST')
            if not cst: cst = get_val(imposto, './/nfe:CSOSN') # Se for Simples Nacional
            
            # Valores Específicos do Item (Busca profunda)
            v_bc_item = get_val(imposto, './/nfe:ICMS//nfe:vBC', float)
            p_icms_item = get_val(imposto, './/nfe:ICMS//nfe:pICMS', float)
            v_icms_item = get_val(imposto, './/nfe:ICMS//nfe:vICMS', float)
            
            p_ipi_item = get_val(imposto, './/nfe:IPI//nfe:pIPI', float)
            v_ipi_item = get_val(imposto, './/nfe:IPI//nfe:vIPI', float)

            row = {
                # CAMPOS SOLICITADOS EXATAMENTE
                'Mês': str(dt.month).zfill(2),
                'Ano': str(dt.year),
                'Chave Acesso NFe': chave,
                'Inscrição Destinatário': get_val(dest, 'nfe:IE'),
                'Inscrição Emitente': get_val(emit, 'nfe:IE'),
                'Razão Social Emitente': get_val(emit, 'nfe:xNome'),
                'Cnpj Emitente': get_val(emit, 'nfe:CNPJ'),
                'UF Emitente': get_val(emit, 'nfe:enderEmit/nfe:UF'),
                'Nr NFe': get_val(ide, 'nfe:nNF'),
                'Série': get_val(ide, 'nfe:serie'),
                'Data NFe': dt.strftime('%d/%m/%Y'),
                
                # Totais
                'BC ICMS Total': v_bc_tot,
                'ICMS Total': v_icms_tot,
                'BC ST Total': v_bcst_tot,
                'ICMS ST Total': v_st_tot,
                'Desc Total': v_desc_tot,
                'IPI Total': v_ipi_tot,
                'Total Produtos': v_prod_tot,
                'Total NFe': v_nf_tot,
                
                # Item
                'Descrição Produto NFe': get_val(prod, 'nfe:xProd'),
                'NCM na NFe': get_val(prod, 'nfe:NCM'),
                'CST': cst,
                'CFOP NFe': get_val(prod, 'nfe:CFOP'),
                'Qtde': get_val(prod, 'nfe:qCom', float),
                'Unid': get_val(prod, 'nfe:uCom'),
                'Vr Unit': get_val(prod, 'nfe:vUnCom', float),
                'Vr Total': get_val(prod, 'nfe:vProd', float),
                'Desconto Item': get_val(prod, 'nfe:vDesc', float),
                
                # Impostos Detalhados do Item
                'Base de Cálculo ICMS': v_bc_item,
                'Aliq ICMS': p_icms_item,
                'Vr ICMS': v_icms_item,
                'Aliq IPI': p_ipi_item,
                'Vr IPI': v_ipi_item
            }
            
            itens_db.append(NFe(
                chave_item=f"{chave}-{i+1}", 
                chave_acesso=chave, 
                ano=str(dt.year),
                mes=str(dt.month).zfill(2),
                cnpj_emitente=get_val(emit, 'nfe:CNPJ'),
                cnpj_destinatario=get_val(dest, 'nfe:CNPJ'),
                dados_json=str(row)
            ))
        return itens_db
    except: return []

# --- ROTAS ---
@app.post("/upload")
async def upload(files: List[UploadFile] = File(...)):
    if os.path.exists(TEMP_DIR): 
        shutil.rmtree(TEMP_DIR)
        os.makedirs(TEMP_DIR)

    c_arquivos = 0
    c_itens = 0
    sess = SessionLocal()

    try:
        for file in files:
            file_path = os.path.join(TEMP_DIR, file.filename)
            with open(file_path, "wb") as b: shutil.copyfileobj(file.file, b)
            try:
                with zipfile.ZipFile(file_path, 'r') as z: z.extractall(TEMP_DIR)
            except: continue

        xml_files = glob.glob(f"{TEMP_DIR}/**/*.xml", recursive=True) + glob.glob(f"{TEMP_DIR}/**/*.XML", recursive=True)
        c_arquivos = len(xml_files)

        for f in xml_files:
            itens = parse_xml(f)
            for item in itens:
                sess.merge(item)
                c_itens += 1
        sess.commit()

        return JSONResponse({"ok": True, "msg": f"{c_arquivos} XMLs lidos. {c_itens} itens processados."})

    except Exception as e:
        sess.rollback()
        return JSONResponse({"ok": False, "msg": str(e)})
    finally:
        sess.close()

@app.get("/filtros")
async def get_filtros():
    s = SessionLocal()
    anos = s.query(NFe.ano).distinct().order_by(NFe.ano).all()
    s.close()
    return {"anos": [x[0] for x in anos]}

@app.get("/historico")
async def get_historico():
    files = []
    for f in os.listdir(REPORTS_DIR):
        if f.endswith(".xlsx"):
            path = os.path.join(REPORTS_DIR, f)
            t = os.path.getmtime(path)
            dt = datetime.fromtimestamp(t).strftime('%d/%m/%Y %H:%M')
            files.append({"nome": f, "data": dt})
    files.sort(key=lambda x: x['nome'], reverse=True)
    return {"arquivos": files}

@app.post("/gerar")
async def gerar(anos: str = Form(...), meu_cnpj: str = Form("")):
    s = SessionLocal()
    try:
        l_anos = anos.split(',')
        res = s.query(NFe).filter(NFe.ano.in_(l_anos)).all()
        if not res: return JSONResponse({"ok": False, "msg": "Sem dados."})

        data = [eval(r.dados_json) for r in res]
        df = pd.DataFrame(data)

        # Lógica para o Resumo na Tela (Entrada vs Saida)
        # Nota: Mantemos isso para a tela, mas NÃO colocamos no Excel pois você pediu ordem exata
        def classificar(row):
            if not meu_cnpj: return "Indefinido"
            cnpj_limpo = ''.join(filter(str.isdigit, meu_cnpj))
            emit = ''.join(filter(str.isdigit, str(row.get('Cnpj Emitente', '')))) # Usando a chave exata
            dest = ''.join(filter(str.isdigit, str(row.get('Destinatário CNPJ', '')))) # Chave pode variar se XML for ruim, mas tentamos
            if emit == cnpj_limpo: return "SAÍDA"
            if dest == cnpj_limpo: return "ENTRADA"
            return "OUTROS"

        if meu_cnpj:
            df['__temp_tipo'] = df.apply(classificar, axis=1)

        # Ordenação Cronológica
        df = df.sort_values(by=['Ano', 'Mês', 'Data NFe'])

        # --- DEFINIÇÃO ESTRITA DAS COLUNAS (ORDEM SOLICITADA) ---
        cols = [
            'Mês', 'Ano', 'Chave Acesso NFe', 'Inscrição Destinatário', 'Inscrição Emitente',
            'Razão Social Emitente', 'Cnpj Emitente', 'UF Emitente', 'Nr NFe', 'Série', 'Data NFe',
            'BC ICMS Total', 'ICMS Total', 'BC ST Total', 'ICMS ST Total', 'Desc Total', 'IPI Total',
            'Total Produtos', 'Total NFe', 'Descrição Produto NFe', 'NCM na NFe', 'CST', 'CFOP NFe',
            'Qtde', 'Unid', 'Vr Unit', 'Vr Total', 'Desconto Item', 
            'Base de Cálculo ICMS', 'Aliq ICMS', 'Vr ICMS', 'Aliq IPI', 'Vr IPI'
        ]
        
        # Garante que só essas colunas saiam e nessa ordem
        df = df.reindex(columns=cols).fillna("")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Relatorio_Fiscal_{timestamp}.xlsx"
        filepath = os.path.join(REPORTS_DIR, filename)
        df.to_excel(filepath, index=False)

        # Prova Real
        v_itens = df['Vr Total'].replace('', 0).astype(float).sum()
        df_unicas = df.drop_duplicates(subset=['Chave Acesso NFe'])
        v_notas = df_unicas['Total NFe'].replace('', 0).astype(float).sum()
        
        # Resumo Entradas/Saidas (Apenas contagem visual)
        resumo_msg = "Sem filtro de CNPJ"
        if meu_cnpj and '__temp_tipo' in df:
            entradas = len(df_unicas[df_unicas['__temp_tipo'] == 'ENTRADA'])
            saidas = len(df_unicas[df_unicas['__temp_tipo'] == 'SAÍDA'])
            resumo_msg = f"Entradas: {entradas} | Saídas: {saidas}"

        return JSONResponse({
            "ok": True,
            "filename": filename,
            "notas": f"R$ {v_notas:,.2f}",
            "itens": f"R$ {v_itens:,.2f}",
            "qtd_notas": len(df_unicas),
            "resumo_ops": resumo_msg,
            "url": f"/download/{filename}"
        })
    finally: s.close()

@app.get("/download/{filename}")
async def download(filename: str):
    path = os.path.join(REPORTS_DIR, filename)
    if os.path.exists(path): return FileResponse(path, filename=filename)
    return JSONResponse({"msg": "Arquivo não encontrado"}, 404)

# --- FRONTEND ---
@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html lang="pt-br">
    <head>
        <meta charset="UTF-8">
        <title>Extrator Fiscal V6</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/sweetalert2@11"></script>
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    </head>
    <body class="bg-slate-50 min-h-screen text-slate-800 font-sans">
        <div class="max-w-6xl mx-auto p-6 grid grid-cols-1 lg:grid-cols-3 gap-6">
            
            <div class="lg:col-span-2 space-y-6">
                <div class="bg-white p-6 rounded-xl shadow border border-slate-200">
                    <h2 class="text-xl font-bold text-blue-700 mb-4"><i class="fas fa-upload"></i> Importar Dados</h2>
                    <div id="dropZone" class="border-2 border-dashed border-slate-300 rounded-lg p-10 text-center hover:bg-blue-50 hover:border-blue-400 transition cursor-pointer group">
                        <i class="fas fa-file-archive text-5xl text-slate-300 group-hover:text-blue-500 mb-3 transition"></i>
                        <p class="font-medium text-slate-600">Arraste múltiplos ZIPs aqui</p>
                        <input type="file" id="fileInput" accept=".zip" multiple class="hidden">
                    </div>
                </div>

                <div class="bg-white p-6 rounded-xl shadow border border-slate-200">
                    <h2 class="text-xl font-bold text-green-700 mb-4"><i class="fas fa-filter"></i> Gerar Relatório</h2>
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                        <div>
                            <label class="block text-sm font-bold text-slate-600 mb-1">Seu CNPJ (Opcional)</label>
                            <input type="text" id="meuCnpj" placeholder="Números apenas" class="w-full border border-slate-300 rounded p-2 focus:ring focus:ring-blue-200 outline-none">
                        </div>
                        <div>
                            <label class="block text-sm font-bold text-slate-600 mb-1">Anos Disponíveis</label>
                            <div id="anosList" class="flex flex-wrap gap-2 text-sm">Carregando...</div>
                        </div>
                    </div>
                    <button id="btnGerar" onclick="gerar()" disabled class="w-full bg-slate-300 text-white font-bold py-3 rounded-lg shadow transition">
                        Selecione um ano
                    </button>
                </div>
            </div>

            <div class="bg-white p-6 rounded-xl shadow border border-slate-200 h-fit">
                <h2 class="text-xl font-bold text-purple-700 mb-4"><i class="fas fa-history"></i> Relatórios Gerados</h2>
                <div id="historicoList" class="space-y-2 max-h-[500px] overflow-y-auto pr-2">
                    <div class="text-center text-slate-400 py-4"><i class="fas fa-circle-notch fa-spin"></i></div>
                </div>
                <button onclick="loadHistorico()" class="mt-4 text-sm text-blue-600 hover:underline w-full text-center"><i class="fas fa-sync"></i> Atualizar</button>
            </div>
        </div>

        <script>
            const dropZone = document.getElementById('dropZone');
            const fileInput = document.getElementById('fileInput');
            dropZone.onclick = () => fileInput.click();
            fileInput.onchange = () => handleFiles(fileInput.files);
            ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(evt => {
                dropZone.addEventListener(evt, e => { e.preventDefault(); e.stopPropagation(); });
            });
            dropZone.addEventListener('drop', e => handleFiles(e.dataTransfer.files));

            async function handleFiles(files) {
                if(!files.length) return;
                let fd = new FormData();
                let hasZip = false;
                for(let i=0; i<files.length; i++) {
                    if(files[i].name.toLowerCase().endsWith('.zip')) {
                        fd.append('files', files[i]);
                        hasZip = true;
                    }
                }
                if(!hasZip) return Swal.fire('Erro', 'Envie ZIPs.', 'error');
                Swal.fire({title: 'Importando...', html: 'Lendo XMLs...', didOpen: () => Swal.showLoading()});
                try {
                    let res = await fetch('/upload', {method:'POST', body:fd});
                    let data = await res.json();
                    if(data.ok) { Swal.fire('Sucesso!', data.msg, 'success'); loadFiltros(); }
                    else { Swal.fire('Erro', data.msg, 'error'); }
                } catch(e) { Swal.fire('Erro', 'Falha na conexão.', 'error'); }
            }

            async function loadFiltros() {
                let res = await fetch('/filtros');
                let data = await res.json();
                let div = document.getElementById('anosList');
                div.innerHTML = '';
                if(data.anos.length === 0) div.innerHTML = 'Nenhum dado.';
                data.anos.forEach(ano => {
                    div.innerHTML += `<label class="cursor-pointer select-none"><input type="checkbox" value="${ano}" onchange="checkBtn()" class="peer sr-only"><span class="px-3 py-1 rounded bg-slate-100 border text-slate-500 peer-checked:bg-green-600 peer-checked:text-white peer-checked:border-green-600 transition text-xs font-bold">${ano}</span></label>`;
                });
            }

            async function loadHistorico() {
                let div = document.getElementById('historicoList');
                try {
                    let res = await fetch('/historico');
                    let data = await res.json();
                    div.innerHTML = '';
                    if(data.arquivos.length === 0) { div.innerHTML = '<div class="text-sm text-slate-400 text-center">Nenhum relatório.</div>'; return; }
                    data.arquivos.forEach(file => {
                        div.innerHTML += `<div class="flex items-center justify-between p-3 bg-slate-50 rounded hover:bg-blue-50 border border-slate-100 transition group"><div><div class="text-sm font-bold text-slate-700 truncate w-40" title="${file.nome}">${file.nome}</div><div class="text-xs text-slate-400">${file.data}</div></div><a href="/download/${file.nome}" class="text-blue-500 hover:text-blue-700 bg-white p-2 rounded-full shadow-sm"><i class="fas fa-download"></i></a></div>`;
                    });
                } catch(e) { div.innerHTML = 'Erro ao carregar.'; }
            }

            function checkBtn() {
                let count = document.querySelectorAll('input[type="checkbox"]:checked').length;
                let btn = document.getElementById('btnGerar');
                if(count > 0) {
                    btn.disabled = false;
                    btn.className = "w-full bg-green-600 hover:bg-green-700 text-white font-bold py-3 rounded-lg shadow transition transform active:scale-95";
                    btn.innerText = "Gerar Relatório";
                } else {
                    btn.disabled = true;
                    btn.className = "w-full bg-slate-300 text-white font-bold py-3 rounded-lg shadow transition cursor-not-allowed";
                    btn.innerText = "Selecione um ano";
                }
            }

            async function gerar() {
                let anos = Array.from(document.querySelectorAll('input[type="checkbox"]:checked')).map(x => x.value).join(',');
                let cnpj = document.getElementById('meuCnpj').value;
                Swal.fire({title: 'Gerando...', html: 'Criando Excel...', didOpen: () => Swal.showLoading()});
                let fd = new FormData();
                fd.append('anos', anos);
                fd.append('meu_cnpj', cnpj);
                try {
                    let res = await fetch('/gerar', {method:'POST', body:fd});
                    let data = await res.json();
                    if(data.ok) {
                        loadHistorico();
                        Swal.fire({
                            title: 'Sucesso!',
                            icon: 'success',
                            html: `<div class="bg-slate-100 p-3 rounded text-left text-sm space-y-2 mb-4"><div class="flex justify-between border-b pb-1"><span>Total Notas:</span> <span class="font-bold text-green-700">${data.notas}</span></div><div class="flex justify-between border-b pb-1"><span>Total Itens:</span> <span class="font-bold text-blue-700">${data.itens}</span></div><div class="text-center font-bold text-slate-600 pt-1">${data.resumo_ops}</div></div>`,
                            confirmButtonText: 'Baixar',
                            showCancelButton: true,
                            cancelButtonText: 'Fechar'
                        }).then((result) => { if(result.isConfirmed) window.location.href = data.url; });
                    } else { Swal.fire('Erro', data.msg, 'error'); }
                } catch(e) { Swal.fire('Erro', 'Falha ao processar.', 'error'); }
            }
            loadFiltros();
            loadHistorico();
        </script>
    </body>
    </html>
    """
