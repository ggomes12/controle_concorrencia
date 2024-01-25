import mysql.connector
from mysql.connector import Error 
from datetime import datetime

def conexao():
    conexao = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        database='tbPratico4'
    )
    return conexao


def abrir_conta():
    
    nome = input('Digite seu nome: ')
    credito_inicial = input('Digite o valor do crédito inicial: ')

    conexao_bd = conexao()
    cursor = conexao_bd.cursor()

    try:
        # Recuperar o ID da maior conta criada
        cursor.execute('CALL criarUser( %s, %s)', [nome, credito_inicial])
        aux = cursor.fetchone()
        print(aux)        
        #print('\nNúmero da conta: ', aux[1], '\nMarca de tempo: ', aux[2], '\nSaldo: ', aux[0])
        opc = int(input('Insira 1 para confirmar a criação da conta, senão digite 0: '))

        
        if opc == 1:
            
            conexao_bd.commit()
        else:
        
            conexao_bd.rollback()

    except Exception as e:

        # tem que fazer rollback em caso de erro
        print(f'Erro: {e}')
    finally:
        # Fechar o cursor e a conexão
        cursor.close()
        conexao_bd.close()
        
        

def transferencia_bancaria():
    conexao_bd = conexao()
    cursor = conexao_bd.cursor()

    cursor.execute("SET autocommit = OFF;")

    try:

        # para validar o rollback
        nome = input('Digite seu nome para teste: ')
        cursor.execute('INSERT INTO Contas VALUES (default, %s, default);', [nome])


        conta_origem = int(input('Digite o numero da conta de origem: '))

        # Verifica a conta de origem...
        query_saldo = "SELECT idConta FROM Contas WHERE idConta = %s FOR UPDATE"
        cursor.execute(query_saldo, (conta_origem,))
        conta_orig = cursor.fetchone()

        if conta_orig is None:
            raise ValueError("Conta de origem nao encontrada.")

        valor_transferencia = float(input('Digite o valor da transferência: '))

        # Verifica o saldo da conta de origem...
        query_saldo = "SELECT Saldo FROM Saldo WHERE Contas_idConta = %s ORDER BY idSaldo DESC LIMIT 1"
        cursor.execute(query_saldo, (conta_origem,))
        saldo_origem = cursor.fetchone()


        # ... e o seu saldo
        if saldo_origem is None or saldo_origem[0] < valor_transferencia:
            raise ValueError("Saldo insuficiente para realizar a transferencia.")

        # #para validar o rollback
        # nome = input('Digite seu nome para teste: ')
        # cursor.execute('INSERT INTO Contas VALUES (default, %s, default);', [nome])

        conta_destino = int(input('Digite o número da conta de destino: '))

        # Verifica se a conta de destino nao eh a mesma da conta de origem
        if conta_destino == conta_origem:
            conexao_bd.rollback()
            raise ValueError("Conta de destino inválida.")

        # #para validar o rollback
        # nome = input('Digite seu nome para testar o rollback: ')
        # cursor.execute('INSERT INTO Contas VALUES (default, %s, default);', [nome])
        #


        # faz a call da procedure no sql
        result = cursor.callproc('RealizarTransferencia', (conta_origem, conta_destino, valor_transferencia, 0, "", 0))
        resultado, marca_tempo, saldo_inicial_origem = result[3], result[4], result[5]

        print('\nMarca de tempo: ', marca_tempo, '\nSaldo da conta de origem: ', saldo_inicial_origem)

        #para verificar o retorno do sql, 0 para sucesso
        #print('\nResultado da transferência: ', resultado)

        if resultado == 0:

            confirmacao = input('Deseja confirmar a transferencia?'
                                '\n(Digite S para confirmar ou outra coisa para cancelar): ')
            if confirmacao.upper() == 'S':
                conexao_bd.commit()
                print('Transferencia confirmada.')
            else:
                conexao_bd.rollback()
                print('Transferencia cancelada.')
        else:
            print('Nao foi possivel realizar a transferencia. Verifique as informacoes fornecidas.')
            conexao_bd.rollback()

    except Exception as e:
        print(f"Erro inesperado durante a transferencia: {e}")
        conexao_bd.rollback()

    finally:
        print("Finalizando...")
        cursor.close()
        conexao_bd.close()


def consultar_saldo():
    conta_id = int(input('Digite o número da conta: '))
    data_inicial = input('Digite a data inicial (formato YYYY-MM-DD): ')
    data_final = input('Digite a data final (formato YYYY-MM-DD): ')

    data_inicial = data_inicial + ' 23:59:59'
    data_final = data_final + ' 23:59:59'
    conexao_bd = conexao()
    cursor = conexao_bd.cursor()

    #operação 1 (debito até o primeiro dia informado):
    query = 'select sum(Valor) from Operacoes where Contas_idConta = %s and tipoOperacao = %s and dateOperacao <= %s'
    cursor.execute(query, [conta_id, 'Debito', data_inicial])
    debito1 = cursor.fetchone()
    
    #operação 2 (debito até o último dia informado):
    query = 'select sum(Valor) from Operacoes where Contas_idConta = %s and  tipoOperacao = %s and dateOperacao <= %s'
    cursor.execute(query, [conta_id, 'Debito', data_final])
    debito2 = cursor.fetchone()
    
    #operação 3 (crédito até o primeiro dia informado):
    query = 'select sum(Valor) from Operacoes where Contas_idConta = %s and  tipoOperacao = %s and dateOperacao <= %s'
    cursor.execute(query, [conta_id, 'credito', data_inicial])
    credito1 = cursor.fetchone()
    
    #operação 4 (crédito até o último dia informado):
    query = 'select sum(Valor) from Operacoes where Contas_idConta = %s and  tipoOperacao = %s and dateOperacao <= %s'
    cursor.execute(query, [conta_id, 'credito', data_final])
    credito2 = cursor.fetchone()

    # Fechar o cursor e a conexão
    cursor.close()
    conexao_bd.close()

    # # #
    saldoFinalDbt = -1.0
    saldoFinalCdt = -1.0
    if debito1[0] != None and debito2[0] != None:

        n1 = float(debito1[0])
        n2 = float(debito2[0])
        saldoFinalDbt = (n1 + n2) / 2.0
    elif debito1[0] != None:

        saldoFinalDbt = float(debito1[0])
    elif debito2[0] != None:

        saldoFinalDbt = float(debito2[0])
    #   #        
    if credito1[0] != None and credito2[0] != None:

        n1 = float(credito1[0])
        n2 = float(credito2[0])
        saldoFinalCdt = (n1 + n2) / 2.0
    elif credito1[0] != None:

        saldoFinalCdt = float(credito1[0])
    elif credito2[0] != None:

        saldoFinalCdt = float(credito2[0])
        
    #   #
    if saldoFinalDbt != -1 and saldoFinalCdt != -1:

        mostrar = saldoFinalCdt - saldoFinalDbt
        print('O saldo do cliente da conta ', conta_id,' no intervalo de tempo mostrado é ', mostrar,'R$')
    elif saldoFinalCdt != -1:

        print('O saldo do cliente da conta ', conta_id,' no intervalo de tempo mostrado é ', saldoFinalCdt,'R$')
    else:

        print('Não há saldo no intervalo informado, ou a pessoa informada não existe!')
    

def consultar_extrato():

    conta_id = int(input('Digite o número da conta: '))
    data_inicial = input('Digite a data inicial (formato YYYY-MM-DD): ')
    data_final = input('Digite a data final (formato YYYY-MM-DD): ')

    data_inicial = data_inicial + ' 00:00:00'
    data_final = data_final + ' 23:59:59'
    conexao_bd = conexao()
    cursor = conexao_bd.cursor()

    #operação 1 (operações de débito dentro das datas informadas):
    query = 'select sum(Valor) from Operacoes where Contas_idConta = %s and tipoOperacao = %s and dateOperacao between %s and %s;'
    cursor.execute(query, [conta_id, 'Debito', data_inicial, data_final])
    debito = cursor.fetchone()
    
    #operação 2 (operações de crédito dentro das datas informadas):
    query = 'select sum(Valor) from Operacoes where Contas_idConta = %s and tipoOperacao = %s and dateOperacao between %s and %s;'
    cursor.execute(query, [conta_id, 'credito', data_inicial, data_final])
    credito = cursor.fetchone()
    
    if debito[0] != None or credito[0] != None:

        if debito[0] != None and credito[0] != None:

            print('\nExtrato da conta dentro do espaço de tempo especificado: ', float(credito[0]) - float(debito[0]))
        elif credito[0] != None:
            
            print('\nExtrato da conta dentro do espaço de tempo especificado: ', float(credito[0]))
        else:
            
            print('\nExtrato da conta dentro do espaço de tempo especificado: ', float(debito[0]))
        query = 'select * from Operacoes where Contas_idConta = %s and dateOperacao between %s and %s;'
        cursor.execute(query, [conta_id, data_inicial, data_final])
        listaOperacoes = cursor.fetchall()
        print('...')
        for i in listaOperacoes:

            print('transação de', i[1], 'R$ realizada no', i[2], 'para a conta ', i[4], 'em', i[3], '(chave da transacao: ', i[0],')')
    else:
        print('Não houve nenhuma operação dentro do espaço de tempo informado!')

    cursor.close()
    conexao_bd.close()


while True:
    print('\nMenu:')
    print('1. Abertura de conta')
    print('2. Transferência bancária')
    print('3. Consultar saldo em um intervalo de tempo')
    print('4. Consultar extrato em um intervalo de tempo')
    print('5. Finalizar o programa')

    opc_menu = input('Escolha uma opção: ')

    if opc_menu == '1':
        
        abrir_conta()
    elif opc_menu == '2':

        transferencia_bancaria()
    elif opc_menu == '3':

        consultar_saldo()
    elif opc_menu == '4':

        consultar_extrato()
    elif opc_menu == '5':

        print('Programa finalizado.')
        break
    else:
        
        print('Opção invalida. Tente novamente.')
