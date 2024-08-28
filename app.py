from flow import Flow
#points = ["noticias", "nomes", "localidades"]
points = ["nomes_basica"]

points = ["noticias", "nomes", "localidades", "nomes_basica", "nomes_faixa"]

if __name__ == "__main__":
    flow = Flow()

    while True:
        option = input("Digite 'e' para extrair, 't' para transformar e carregar, 'q' para sair: ")
        
        match option:
            case "e":
                flow.extract_flow(points)
            case "t":
                flow.transform_and_load(points)
            case "q":
                flow.db.close_connection()
                break
            case _:
                print("Opção inválida")
                continue
    
