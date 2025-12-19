const axios = require('axios');
const xlsx = require('xlsx');
require('dotenv').config();

const KEY_BEARER = process.env.KEY_BEARER;
const BASE_URL = process.env.BASE_URL;
const BASE_URL_GROUPS = process.env.BASE_URL_GROUPS;

// Cache para evitar requisições duplicadas
const cache = {
    people: new Map(),
    groups: new Map()
};

// Função com delay para não sobrecarregar API
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

async function fetchData() {
    try {
        console.time('Tempo total de execução');
        
        if (!BASE_URL_GROUPS) {
            throw new Error('BASE_URL_GROUPS não definida');
        }

        const rows = [];
        rows.push([
            'Congregacao',
            'Data Cadastro',
            'Nome',
            'Pai',
            'Mãe',
            'CPF',
            'Nascimento',
            'Naturalidade',
            'Função',
            'Batismo',
            'Rua',
            'Número',
            'Bairro',
            'CEP',
            'Contato',
        ]);

        const axiosHeaders = {
            headers: {
                Authorization: KEY_BEARER,
                Accept: 'application/json'
            }
        };

        // 1. Buscar todos os grupos de uma vez
        console.log('Buscando grupos...');
        const responseGroups = await axios.get(BASE_URL_GROUPS, axiosHeaders);
        const groupsData = responseGroups.data.results;
        
        console.log(`${groupsData.length} grupos encontrados`);

        // 2. Processar grupos em batches (lotes)
        const batchSize = 10; // Processa 10 grupos por vez
        const allPeopleRows = [];
        
        for (let i = 0; i < groupsData.length; i += batchSize) {
            const batch = groupsData.slice(i, i + batchSize);
            console.log(`Processando lote ${Math.floor(i/batchSize) + 1} de ${Math.ceil(groupsData.length/batchSize)}`);
            
            // Processa grupos em paralelo
            const batchPromises = batch.map(group => processGroup(group, axiosHeaders));
            const batchResults = await Promise.all(batchPromises);
            
            // Adiciona resultados
            batchResults.forEach(result => {
                if (result.rows && result.rows.length > 0) {
                    if (allPeopleRows.length > 0) {
                        allPeopleRows.push(['']); // Linha em branco entre grupos
                    }
                    allPeopleRows.push(...result.rows);
                }
            });
            
            // Pequeno delay entre batches para não sobrecarregar API
            if (i + batchSize < groupsData.length) {
                await delay(500);
            }
        }

        // 3. Adicionar todas as linhas ao array principal
        rows.push(...allPeopleRows);

        // 4. Gerar XLSX
        console.log('Gerando arquivo Excel...');
        const ws = xlsx.utils.aoa_to_sheet(rows);
        const colWidths = [25, 20, 35, 35, 35, 15, 10, 25, 20, 10, 40, 10, 30, 10, 15];
        ws['!cols'] = colWidths.map(width => ({ wch: width }));

        const wb = xlsx.utils.book_new();
        xlsx.utils.book_append_sheet(wb, ws, 'Data');

        const filename = `DADOS_ENUVENS_${new Date().toISOString().slice(0, 19).replace('T', ' ')}.xlsx`;
        xlsx.writeFile(wb, filename);

        console.log(`Arquivo gerado com ${(allPeopleRows.length) + 1} linhas!`);

        // Verificar se arquivo foi criado
        const fs = require('fs');
        if (fs.existsSync(filename)) {
            const stats = fs.statSync(filename);
            console.log(`Tamanho do arquivo: ${(stats.size / 1024 / 1024).toFixed(2)} MB`);
        }

        console.timeEnd('Tempo total de execução');

    } catch (error) {
        console.error('Erro:', error.message);
        return null;
    }
}

// Função para processar um grupo específico
async function processGroup(group, headers) {
    try {
        const nomeCongregacao = group?.name ?? 'N/A';
        const groupRows = [];

        // Verifica cache
        const cacheKey = `group_${group.id}`;
        if (cache.groups.has(cacheKey)) {
            return cache.groups.get(cacheKey);
        }

        // Buscar membros do grupo
        const responseMembros = await axios.get(`${BASE_URL}/groups/${group.id}`, headers);

        const membrosData = responseMembros.data.results;
        const peoples = JSON.parse(membrosData.peoples || '[]');

        if (peoples.length === 0) {
            const result = { rows: [], count: 0 };
            cache.groups.set(cacheKey, result);
            return result;
        }

        console.log(`${nomeCongregacao}: ${peoples.length} pessoas`);

        // Processar pessoas em batches (50 por vez)
        const peopleBatchSize = 50;
        const allPeopleData = [];

        for (let i = 0; i < peoples.length; i += peopleBatchSize) {
            const peopleBatch = peoples.slice(i, i + peopleBatchSize);
            
            // Busca todas as pessoas deste batch em paralelo
            const peoplePromises = peopleBatch.map(personId => 
                fetchPersonWithCache(personId, headers)
            );
            
            const batchResults = await Promise.allSettled(peoplePromises);
            
            // Processa resultados bem-sucedidos
            batchResults.forEach(result => {
                if (result.status === 'fulfilled' && result.value) {
                    allPeopleData.push(result.value);
                }
            });

            // Delay entre batches
            if (i + peopleBatchSize < peoples.length) {
                await delay(200);
            }
        }

        // Transformar dados das pessoas em linhas
        allPeopleData.forEach(personData => {
            if (personData) {
                const cpf = personData.doc_1?.replace(/^(\d{3})(\d{3})(\d{3})(\d{2})$/, '$1.$2.$3-$4').slice(0, 14);
                
                groupRows.push([
                    nomeCongregacao.toUpperCase(),
                    formatarData(personData.created_at),
                    personData.full_name?.toUpperCase(),
                    personData.nomePai?.toUpperCase(),
                    personData.nomeMae?.toUpperCase(),
                    cpf,
                    formatarData(personData.birthydate),
                    personData.naturalidade?.toUpperCase(),
                    personData.funcao,
                    formatarData(personData.baptism_date),
                    personData.address_1?.toUpperCase() ?? '',
                    personData.address_number ?? 0,
                    personData.address_2?.toUpperCase() ?? '',
                    personData.postal_code,
                    personData.phone_1
                ]);
            }
        });

        const result = { rows: groupRows, count: groupRows.length };
        cache.groups.set(cacheKey, result);
        
        return result;

    } catch (error) {
        console.error(`Erro no grupo ${group.id}:`, error.message);
        return { rows: [], count: 0 };
    }
}

// Função para buscar pessoa com cache
async function fetchPersonWithCache(personId, headers) {
    const cacheKey = `person_${personId}`;
    
    if (cache.people.has(cacheKey)) {
        return cache.people.get(cacheKey);
    }

    try {
        const response = await axios.get(
            `${BASE_URL}/people/${personId}`,
            headers
        );

        const peopleData = response.data.results;
        
        if (!peopleData) {
            return null;
        }

        // Processar extrafields
        const extrafields = JSON.parse(peopleData.extrafields || '[]');
        const nomePai = extrafields.find(f => f.id_ef === 15819)?.value?.trim() || '';
        const nomeMae = extrafields.find(f => f.id_ef === 15820)?.value?.trim() || '';
        const naturalidade = extrafields.find(f => f.id_ef === 15823)?.value?.trim() || '';
        const funcao = determinarFuncao(extrafields);

        const processedData = {
            ...peopleData,
            nomePai,
            nomeMae,
            naturalidade,
            funcao
        };

        cache.people.set(cacheKey, processedData);
        return processedData;

    } catch (error) {
        console.error(`Erro pessoa ${personId}:`, error.message);
        return null;
    }
}

// Formatar data
function formatarData(data) {
    if (!data) return "";
    
    if (data.length > 10) {
        const [ano, mes, dia, hora] = data.split(/[- ]/);
        return `${dia}/${mes}/${ano} ${hora}`;
    } else {
        const [ano, mes, dia] = data.split("-");
        return `${dia}/${mes}/${ano}`;
    }
}

function determinarFuncao(extrafields) {
    const funcoes = ['MEMBRO', 'COOPERADOR', 'DIÁCONO', 'PRESBÍTERO', 'EVANGELISTA', 'PASTOR', 'CONGREGADO'];
    const campoFuncoes = extrafields.find(item => item.id_ef === "15822");
    
    if (!campoFuncoes || !campoFuncoes.sub) return 'CAMPO NÃO ENCONTRADO';
    
    const indexAtivo = campoFuncoes.sub.findIndex(sub => sub.value === true);
    return indexAtivo !== -1 ? funcoes[indexAtivo] : 'NENHUMA FUNÇÃO';
}

// Executar
setTimeout(() => {
    fetchData();
}, 1000);