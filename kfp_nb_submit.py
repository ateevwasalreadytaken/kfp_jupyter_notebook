import datetime
import kfp.compiler as compiler
import kfp.dsl as dsl
import kfp
import os

tmp_dir = 'temp' #Temporary directory here
image = 'image' #Docker Image for pipeline here

if not os.path.exists(tmp_dir):
    os.makedirs(tmp_dir)
    
def run_notebook(input_notebook: str, output_notebook: str,
                 cpu: str, memory: str, gpu = None, vendor = None):
    def demo_op(input_notebook: str, output_notebook: str):
        return dsl.ContainerOp(
            name='papermill',
            image=image,
            command=['sh', '-c'],
            pvolumes={"Mount": dsl.PipelineVolume(pvc="xyz",name='xyz')},  #Mount here and replace xyz
            arguments=['papermill $0 $1', input_notebook, output_notebook]
        )
    @dsl.pipeline(
        name='papermill demo',
        description='executing notebooks demo'
    )
    def pipeline_func(input_notebook: str, output_notebook: str):
    
        demo_task = demo_op(input_notebook, output_notebook)
        if gpu != None:
            if vendor != None:
                demo_task.set_gpu_limit(gpu, vendor) #default vendor is NVIDIA
            else:
                demo_task.set_gpu_limit(gpu) #number
        demo_task.set_memory_limit(memory) #number followed by 'G' or 'M' etc.
        demo_task.set_cpu_limit(cpu) #number, optionally followed by m indicateing 1/1000
        
        
    filename = tmp_dir + '/demo{dt:%Y%m%d_%H%M%S}.pipeline.tar.gz'.format(dt=datetime.datetime.now())
    print('filename: {}'.format(filename))
    compiler.Compiler().compile(pipeline_func, filename)
    client = kfp.Client()
    experiment = client.create_experiment('name') #name of experiment
    arguments = {'input_notebook': input_notebook, 'output_notebook': output_notebook}
    run_name = 'name' #name of demo run
    run_result = client.run_pipeline(experiment.id, run_name, filename, arguments)
